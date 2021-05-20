package node

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/dlock"
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

func (en *ExtentNode) copyRemoteExtent(conn *grpc.ClientConn, extentID uint64, targetFile *os.File) error{
	c := pb.NewExtentServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()
	copyStream, err := c.CopyExtent(ctx, &pb.CopyExtentRequest{
		ExtentID: extentID,
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
			if _, err = targetFile.Write(payload); err != nil {
				return err
			}
			n += len(payload)
		} else {
			utils.AssertTrue(err == io.EOF)
			break
		}
		xlog.Logger.Infof("recevied data %d", len(payload))
	}

	//check header len
	if n != int(header.PayloadLen) {
		return err
	}

	return nil
}

func (en *ExtentNode) recoveryReplicateExtent(extentInfo *pb.ExtentInfo, task *pb.RecoveryTask) string{

	conn := en.chooseAliveNode(extentInfo, task.ReplaceID)
	if conn == nil {
		xlog.Logger.Warnf("runRecoveryTask: can not find remote connect")
		return ""
	}

	//choose one disk
	i := rand.Intn(len(en.diskFSs))
	targetFile, targetFilePath, err := en.diskFSs[i].AllocCopyExtent(extentInfo.ExtentID)
	defer targetFile.Close()
	if err != nil {
		xlog.Logger.Warnf("can not create CopyExtent copy target [%s]", err.Error())
		os.Remove(targetFilePath)
		return ""
	}

	if err = en.copyRemoteExtent(conn, extentInfo.ExtentID, targetFile) ; err != nil {
		xlog.Logger.Warnf("recoveryReplicateExtent: [%s]", err.Error())
		os.Remove(targetFilePath)
		return ""
	}

    return targetFilePath
}

func (en *ExtentNode) reportRecoveryResult(extentInfo *pb.ExtentInfo, task *pb.RecoveryTask) {

}

func (en *ExtentNode) recoveryErasureExtent(extentInfo *pb.ExtentInfo, task *pb.RecoveryTask) string {

	conns, missingIndex := en.chooseECAliveNode(extentInfo, task.ReplaceID)
	if conns == nil {
		xlog.Logger.Warnf("ErasureExtent: can not find remote nodes")
		return ""
	}
	if missingIndex == -1 {
		xlog.Logger.Warnf("task.ReplaceID is %d, not find int extentInfo", task.ReplaceID)
		return ""
	}

	//FIXME: do not use tmp dir
	tmpDir, err := ioutil.TempDir(os.TempDir(), "recoveryEC")
	if err != nil {
		xlog.Logger.Warnf("ErasureExtent: can not create tmpDir")
		os.RemoveAll(tmpDir)
	}
	defer os.RemoveAll(tmpDir)

	tmpFiles := make([]*os.File, len(conns))
	for i := range conns {
		if conns[i] != nil {
			f, err := os.Create(fmt.Sprintf("%s/%d", tmpDir, i))
			if err != nil {
				return ""
			}
			tmpFiles[i] = f
		}
	}

	stopper := utils.NewStopper()
	var completes int32
	for i := range conns {
		j := i
		stopper.RunWorker(func() {
			if conns[j] == nil {
				return
			}
			if err = en.copyRemoteExtent(conns[j], extentInfo.ExtentID, tmpFiles[j]); err != nil {
				xlog.Logger.Warnf("ErasureExtent can not copyRemoteExtent %v", err)
				return
			}
			atomic.AddInt32(&completes,1)
		})
	}
	stopper.Wait()

	if completes != int32(len(extentInfo.Replicates)) {
		return ""
	}

	//create targetFile
	//choose one disk
	i := rand.Intn(len(en.diskFSs))
	targetFile, targetFilePath, err := en.diskFSs[i].AllocCopyExtent(extentInfo.ExtentID)
	defer targetFile.Close()
	if err != nil {
		xlog.Logger.Warnf("can not create copy target [%s]", err.Error())
		os.Remove(targetFilePath)
		return ""
	}
	//preapre input and output
	input := make([]io.Reader, len(conns))
	for i := range input {
		input[i] = tmpFiles[i]
	}

	output := make([]io.Writer, len(conns))
	output[missingIndex] = targetFile

	//successfull to get data, reconstruct

	err = erasure_code.ReedSolomon{}.Reconstruct(input, len(extentInfo.Replicates), len(extentInfo.Parity),  output)
	if err != nil {
		xlog.Logger.Warnf("node recovery: doing ereasure reconstruct [%s]", err.Error())
		os.Remove(targetFilePath)
		return ""
	}

	return targetFilePath
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
		if extentInfo.Replicates[i] == except {
			continue
		}
		pool , err := conn.GetPools().Get(addrs[i])
		if err != nil {
			continue
		}
		return pool.Get()
	}

	return nil
}

func (en *ExtentNode) chooseECAliveNode(extentInfo *pb.ExtentInfo, except uint64) ([]*grpc.ClientConn, int){
	var missingIndex = -1

	addrs := en.em.GetPeers(extentInfo.ExtentID)
	if addrs == nil {
		return nil, missingIndex
	}
	utils.AssertTrue(len(addrs) == len(extentInfo.Replicates) + len(extentInfo.Parity))
	activeConns := 0
	conns := make([]*grpc.ClientConn, len(addrs))

	nodes := make([]uint64, len(addrs))
	copy(nodes, extentInfo.Replicates)
	copy(nodes[len(extentInfo.Replicates):], extentInfo.Parity)
	for i := 0;i  < len(nodes);i ++ {
		//skip
		if nodes[i] == except {
			missingIndex = i
			continue
		}
		pool , err := conn.GetPools().Get(addrs[i])
		if err != nil {
			conns[i] = nil
		} else {
			conns[i] = pool.Get()
			activeConns++
		}
	}

	if activeConns == len(extentInfo.Replicates) {
		return conns, missingIndex
	}

	return nil, missingIndex
}


func (en *ExtentNode) runRecoveryTask(lock *dlock.DLock, task *pb.RecoveryTask) {
	go func() {
		defer lock.Close()
		defer func() {
			//dec
			atomic.AddInt32(&en.recoveryTaskNum, ^int32(0))
		}()

		extentInfo := en.em.Update(task.ExtentID) //get the latest extentInfo

		//FIXME: valid task
		if extentInfo.IsSealed == 0 {
			return
		}

		count := 0
		for i := range extentInfo.Replicates {
			if extentInfo.Replicates[i] == task.ReplaceID {
				count ++
			}
		}

		for i := range extentInfo.Parity {
			if extentInfo.Replicates[i] == task.ReplaceID {
				count ++
			}
		}

		if count != 0 {
			xlog.Logger.Warnf("remote request %v is not right", task)
			return 
		}

		isEC := len(extentInfo.Parity) > 0
		var targetFilePath string
		if isEC == false {
			targetFilePath = en.recoveryReplicateExtent(extentInfo, task)
		} else {
			targetFilePath = en.recoveryErasureExtent(extentInfo, task)
		}
		//if any error happend
		if len(targetFilePath) == 0 {
			return
		}
		
		//add targetFilePath to extent
		ex, err := extent.OpenExtent(targetFilePath)
		if err != nil {
			xlog.Logger.Warnf("can not open copied extent [%v]", err)
			os.Remove(targetFilePath)
			return
		}
		//FIXME: cleanup LOCK
		//defer unlock cleanup lock
		en.setExtent(ex.ID, ex)


        //report to leader
		ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
		defer cancel()
		err = en.smClient.CopyExtentDone(ctx, task.ExtentID, task.ReplaceID, en.nodeID)
		if err != nil {
			xlog.Logger.Warnf("try to call CopyExtentDone failed [%v]", err)
			en.removeExtent(ex.ID)
			ex.Close()
			os.Remove(targetFilePath)
			return
		}
		
		xlog.Logger.Infof("success copy extent %d", task.ExtentID)
	}()
}


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

	extent := en.getExtent(req.ExtentID)
	if extent == nil {
		return errDone(errors.New("no such extentID"), stream)
	}
	//FIXME:这里可能需要检查是否seal是最新的
	//因为之前的调用seal命令可能失败
	if extent.IsSeal() == false {
		//update stateInfo
		//get commitlength
	}

	stream.Send(&pb.CopyExtentResponse{
		Data:&pb.CopyExtentResponse_Header{
			Header: &pb.CopyResponseHeader{
				Code: pb.Code_OK,
				PayloadLen: uint64(extent.CommitLength()),
			},
		},
	})

	reader := extent.GetReader()
	buf := make([]byte, 512<<10)
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

	lockName := stream_manager.FormatRecoveryTaskLock(req.Task.ExtentID)

	taskLock := dlock.NewDLock(lockName)

	//trylock
	if err := taskLock.Lock(50 * time.Millisecond); err != nil {
		//https://github.com/etcd-io/etcd/issues/10096#issuecomment-423672401
		taskLock.Close()
		return errDone(err)
	}

	n := atomic.LoadInt32(&en.recoveryTaskNum)
	if n < MaxConcurrentTask && atomic.CompareAndSwapInt32(&en.recoveryTaskNum, n, n+1) {
		//reply accept
		en.runRecoveryTask(taskLock, req.Task)

		return &pb.RequireRecoveryResponse{
			Code: pb.Code_OK,
		}, nil
	}

	//reply will not accept
	return errDone(errors.New("exceed MaxConcurrentTask, please wait..."))
}