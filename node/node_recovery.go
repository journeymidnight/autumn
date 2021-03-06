package node

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/erasure_code"
	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/manager/stream_manager"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)


func (en *ExtentNode) copyRemoteExtent(conn *grpc.ClientConn, exInfo *pb.ExtentInfo, targetWriter io.WriteSeeker, offset uint64, size uint64) error{
	c := pb.NewExtentServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	copyStream, err := c.CopyExtent(ctx, &pb.CopyExtentRequest{
		ExtentID: exInfo.ExtentID,
		Eversion: exInfo.Eversion,
		Offset: offset,
		Size_: size,
	})
	//copyStream will close when met non-nil error

	if err != nil {
		return err
	}

	var header *pb.CopyResponseHeader
	//
	res, err := copyStream.Recv()
	if err != nil {
		return err
	}
	if res.GetHeader() == nil {
		return errors.New("copyRemoteExtent: GetHeader is nil")
	}

	header = proto.Clone(res.GetHeader()).(*pb.CopyResponseHeader)

	n := 0
	for {
		res, err := copyStream.Recv()
		if err != nil && err != io.EOF {
			return err
		}
		payload := res.GetPayload()
		if len(payload) > 0 {
			if _, err = targetWriter.Write(payload); err != nil {
				return err
			}
			n += len(payload)
		} else {
			utils.AssertTrue(err == io.EOF)
			break
		}
		xlog.Logger.Debugf("recevied data %d", len(payload))
	}

	//check header len
	if n != int(header.PayloadLen) {
		return errors.Errorf("header is %v, got data length %d", header,n)
	}

	//rewind to start for reading
	targetWriter.Seek(0, os.SEEK_SET)
	return nil
}

//func (en *ExtentNode) recoveryReplicateExtent(extentInfo *pb.ExtentInfo, task *pb.RecoveryTask, targetWriter *os.File) error {
func (en *ExtentNode) recoveryReplicateExtent(exInfo *pb.ExtentInfo, exceptID, offset, size uint64, targetWriter io.WriteSeeker) error {

	conn := en.chooseAliveNode(exInfo, exceptID)

	if conn == nil {
		xlog.Logger.Warnf("runRecoveryTask: can not find remote connect")
		return errors.Errorf("runRecoveryTask: can not find remote connect")
	}

	if err := en.copyRemoteExtent(conn, exInfo , targetWriter, 0, exInfo.SealedLength) ; err != nil {
		xlog.Logger.Warnf("recoveryReplicateExtent: [%s]", err.Error())
		return  err
	}
    return nil
}

func (en *ExtentNode) recoveryErasureExtent(exInfo *pb.ExtentInfo, exceptID, offset, size uint64, targetExtent *extent.Extent) error {

	conns, replacingIndex, err := en.chooseECAliveNode(exInfo, exceptID)
	if err != nil {
		return err
	}
	if replacingIndex == -1 {
		return errors.Errorf("task.ReplaceID is %d, not find in extentInfo", exceptID)
	}

	//FIXME: do not use tmp dirs
	tmpDir, err := ioutil.TempDir(os.TempDir(), "recoveryEC")
	if err != nil {
		xlog.Logger.Warnf("ErasureExtent: can not create tmpDir")
		os.RemoveAll(tmpDir)
	}
	defer os.RemoveAll(tmpDir)

	sourceExtent := make([]*extent.Extent, len(conns))

	for i := range sourceExtent {
		if i != replacingIndex {
			//ID 0 indicate the extent is tmp extent
			ex, err := extent.CreateExtent(fmt.Sprintf("%s/%d", tmpDir, i), 0)
			if err != nil {
				return err
			}
			sourceExtent[i] = ex
		}else {
			sourceExtent[i] = nil
		}
	}

	stopper := utils.NewStopper()

	var completes int32
	for i := range sourceExtent {
		if i == replacingIndex {
			continue
		}
		j := i
		stopper.RunWorker(func() {
			ex := sourceExtent[j]
			ex.Lock()
			defer ex.Unlock()
			if err := en.copyRemoteExtent(conns[j], exInfo, ex.GetRawWriter(), offset, size); err != nil  {
				xlog.Logger.Warnf("ErasureExtent can not copyRemoteExtent %v", err)
				sourceExtent[j] = nil
				return
			}
			atomic.AddInt32(&completes,1)
		})
	}
	stopper.Wait()

	if completes != int32(len(exInfo.Replicates)) {
		return errors.Errorf("can not call enought shards")
	}

	return erasure_code.ReedSolomon{}.RebuildECExtent(len(exInfo.Replicates), len(exInfo.Parity), sourceExtent, replacingIndex, targetExtent)

}



//choose at least n alive node from extentInfo
//replics: return one connection
//EC: return (datashards+parity) connections
func (en *ExtentNode) chooseAliveNode(extentInfo *pb.ExtentInfo, except uint64) *grpc.ClientConn {
	addrs := en.em.GetPeers(extentInfo.ExtentID)
	if addrs == nil {
		return nil
	}
	utils.AssertTrue(len(addrs) == len(extentInfo.Replicates))
	for i := range extentInfo.Replicates {
		if extentInfo.Replicates[i] == except || (1 << i) & extentInfo.Avali == 0 {
			continue
		}
		pool := conn.GetPools().Connect(addrs[i])
		if pool == nil || !pool.IsHealthy(){
			continue
		}
		return pool.Get()
	}

	return nil
}

func (en *ExtentNode) chooseECAliveNode(extentInfo *pb.ExtentInfo, except uint64) ([]*grpc.ClientConn, int, error){
	var missingIndex = -1

	addrs := en.em.GetPeers(extentInfo.ExtentID)
	if addrs == nil {
		return nil, 0, errors.New("can not get enough addrs")
	}
	utils.AssertTrue(len(addrs) == len(extentInfo.Replicates) + len(extentInfo.Parity))
	activeConns := 0
	conns := make([]*grpc.ClientConn, len(addrs))

	nodes := make([]uint64, len(addrs))
	copy(nodes, extentInfo.Replicates)
	copy(nodes[len(extentInfo.Replicates):], extentInfo.Parity)
	
	for i := 0;i  < len(nodes);i ++ {
		//skip
		if nodes[i] == except || (1 << i) & extentInfo.Avali == 0{
			missingIndex = i
			continue
		}

		if (1 << i) & extentInfo.Avali == 0 {
			continue
		}

		pool := conn.GetPools().Connect(addrs[i])
		if pool == nil || !pool.IsHealthy() {
			conns[i] = nil
		} else {
			conns[i] = pool.Get()
			activeConns++
		}

	}

	fmt.Printf("activeConns is %d\n", activeConns)
	if activeConns == len(extentInfo.Replicates) {
		return conns, missingIndex, nil
	}

	return nil, missingIndex, errors.New("can not find enough nodes to recover")
}


func (en *ExtentNode) runRecoveryTask(task *pb.RecoveryTask, extentInfo *pb.ExtentInfo, copyFilePath string, diskID uint64) {
	
		atomic.AddInt32(&en.recoveryTaskNum, 1)
		defer func(){
			atomic.AddInt32(&en.recoveryTaskNum, ^int32(0))
		}()

		isEC := len(extentInfo.Parity) > 0
		var err error
		//loop 
		for {
			//有可能manager等的太久了,或者网络parttion, 找另外一个
			//node做完了recovery任务, 这个任务自动取消
			if stream_manager.FindNodeIndex(extentInfo, task.ReplaceID) == -1  {
				os.Remove(copyFilePath)
				return
			}
			//write to ".copy" file, and 
			if isEC == false {
				copyFile, err  := os.OpenFile(copyFilePath, os.O_RDWR, 0666)
				if err!= nil {
					xlog.Logger.Errorf(err.Error())
					return
				}
				err = en.recoveryReplicateExtent(extentInfo, task.ReplaceID, 0, extentInfo.SealedLength, copyFile)
				copyFile.Close()
			} else {
				ex , err := extent.OpenExtent(copyFilePath)
				if err != nil {
					xlog.Logger.Warnf(err.Error())
					return 
				}
				err = en.recoveryErasureExtent(extentInfo, task.ReplaceID, 0, extentInfo.SealedLength, ex)
				ex.Close()
			}

			if err == nil {
				break
			}
			xlog.Logger.Warnf(err.Error())

			time.Sleep(60*time.Second)
			//get the latest extentInfo
			extentInfo = en.em.Latest(task.ExtentID)
		}

		//rename file from XX.XX.copy to XX.ext
		extentFileName := fmt.Sprintf("%s.ext", path.Dir(copyFilePath))
		utils.Check(os.Rename(copyFilePath, extentFileName))
		ex, err := extent.OpenExtent(extentFileName)
		utils.Check(err)
		en.setExtent(ex.ID, &ExtentOnDisk{
			Extent: ex,
			diskID: diskID,
		})
}


//
func (en *ExtentNode) CopyExtent(req *pb.CopyExtentRequest, stream pb.ExtentService_CopyExtentServer) error {
	errDone := func(err error, stream pb.ExtentService_CopyExtentServer) (error) {
		code, desCode := wire_errors.ConvertToPBCode(err)
		ret := pb.CopyExtentResponse{
			Data:&pb.CopyExtentResponse_Header{
				Header: &pb.CopyResponseHeader{
					Code: code,
					CodeDes: desCode,
				},
			},
		}
		stream.Send(&ret)
		return io.EOF
	}


	extentInfo := en.em.WaitVersion(req.ExtentID, req.Eversion)
	if extentInfo == nil {
		return errDone(errors.New("no such extentInfo"), stream)
	}

	//BUGONs
	extent := en.getExtent(req.ExtentID)
	if extent == nil {
		return errDone(errors.Errorf("node %d do not have extent %d", en.nodeID, req.ExtentID), stream)
	}
	if !extent.IsSeal() {
		return errDone(errors.Errorf("BUGON: extent %d on node %d is not sealed", req.ExtentID, en.nodeID), stream)
	}
	if req.Offset + req.Size_ > uint64(extent.CommitLength()) {
		return errDone(errors.Errorf("BUGON: extent %d on node %d's length is %d , less than requested", req.ExtentID, en.nodeID, extent.CommitLength()), stream)
	}
	

	stream.Send(&pb.CopyExtentResponse{
		Data:&pb.CopyExtentResponse_Header{
			Header: &pb.CopyResponseHeader{
				Code: pb.Code_OK,
				PayloadLen: req.Size_,
			},
		},
	})

	fmt.Printf("extent size is %d, transfter size %d\n", extent.CommitLength(), req.Size_)

	reader := extent.GetReader()
	reader.Seek(int64(req.Offset), io.SeekStart)
	buf := make([]byte, 512 << 10)
	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		if err = stream.Send(&pb.CopyExtentResponse{
			Data:&pb.CopyExtentResponse_Payload{
				Payload: buf[:n],
			},
		}); err != nil {
			return err
		}
		
		time.Sleep(1 * time.Millisecond)
	}
	return nil
}



func (en *ExtentNode) RequireRecovery(ctx context.Context, req *pb.RequireRecoveryRequest) (*pb.RequireRecoveryResponse, error) {

	errDone := func(err error) (*pb.RequireRecoveryResponse, error) {
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.RequireRecoveryResponse{
			Code:    code,
			CodeDes: desCode,
		}, nil
	}

	n := atomic.LoadInt32(&en.recoveryTaskNum)
	if n < MaxConcurrentTask {
		//reply accept

		if en.getExtent(req.Task.ExtentID) != nil {
			return errDone(errors.New("overlapping current file"))
		}
		//create
		//xlog.Logger.Infof("run recovery task %+v on node %d\n", req.Task, en.nodeID)

		//wait for the latest extentInfo
		extentInfo := en.em.Latest(req.Task.ExtentID)

		if extentInfo.SealedLength == 0 {
			return errDone(errors.New("extent should be sealed"))
		}

		if stream_manager.FindNodeIndex(extentInfo, req.Task.ReplaceID) == -1  {
			xlog.Logger.Infof("task %v is not valid to info %v", req.Task, extentInfo)
			return errDone(errors.Errorf("task %v is not valid to info %v", req.Task, extentInfo))
		}

		//create targetFile
		//choose one disk
		i := en.chooseDisktoAlloc()
		targetFilePath, err := en.diskFSs[i].AllocCopyExtent(extentInfo.ExtentID, req.Task.ReplaceID)
		if err != nil {
			xlog.Logger.Warnf("can not create CopyExtent copy target [%s]", err.Error())
			return errDone(err)
		}
		go en.runRecoveryTask(req.Task, extentInfo, targetFilePath, i)

		return &pb.RequireRecoveryResponse{
			Code: pb.Code_OK,
		}, nil
	}

	//reply will not accept
	return errDone(errors.New("exceed MaxConcurrentTask, please wait..."))
}

func (en *ExtentNode) ReAvali(ctx context.Context, req *pb.ReAvaliRequest) (*pb.ReAvaliResponse, error) {
	errDone := func(err error) (*pb.ReAvaliResponse, error) {
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.ReAvaliResponse{
			Code:    code,
			CodeDes: desCode,
		}, nil
	}

	
	exInfo := en.em.WaitVersion(req.ExtentID, req.Eversion)

	//en will truncated and seal extent in en.extentInfoUpdatedfunc
	ex := en.getExtent(req.ExtentID)
	if ex == nil {
		//BUGON
		xlog.Logger.Errorf("BUGON: can not find extent %d", req.ExtentID)
		return errDone(errors.Errorf("can not find extent %d",req.ExtentID))
	}

	ex.Lock()
	defer ex.Unlock()

	if ex.IsSeal() {
		return &pb.ReAvaliResponse{
			Code: pb.Code_OK,
		}, nil
	}

	if err := ex.Seal(uint32(exInfo.SealedLength)) ; err == nil {
		return &pb.ReAvaliResponse{
			Code: pb.Code_OK,
		}, nil
	}

	//extent's length is less than CommitLength...
	xlog.Logger.Warnf("Reavali: extent's length is %d, commitLength is %d", ex.CommitLength(), exInfo.SealedLength)
	utils.AssertTrue(ex.CommitLength() < uint32(exInfo.SealedLength))


	isEC := len(exInfo.Parity) > 0

	var err error
	if isEC == false {
		offset := uint64(ex.CommitLength())
		size := exInfo.SealedLength - uint64(ex.CommitLength())
		appendWriter := ex.GetRawWriter()
		err = en.recoveryReplicateExtent(exInfo, en.nodeID, offset, size, appendWriter)
	} else {
		start, _ := ex.ValidAllBlocks()
		err = ex.Truncate(uint32(start))
		if err == nil {
			err = en.recoveryErasureExtent(exInfo, en.nodeID, uint64(start), exInfo.SealedLength - start, ex.Extent)
		}
	}
	xlog.Logger.Infof("recovery data err is %v", err)
	if err != nil {
		return errDone(err)
	}
	
	utils.Check(ex.Seal(uint32(exInfo.SealedLength)))


	return &pb.ReAvaliResponse{
		Code: pb.Code_OK,
	}, nil

}