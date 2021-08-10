/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless  by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package node

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/erasure_code"
	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

var (
	_                 = conn.GetPools
	_                 = fmt.Printf
	MaxConcurrentTask = int32(2)
)

//internal services
func (en *ExtentNode) Heartbeat(in *pb.Payload, stream pb.ExtentService_HeartbeatServer) error {
	ticker := time.NewTicker(conn.EchoDuration)
	defer ticker.Stop()

	ctx := stream.Context()
	out := &pb.Payload{Data: []byte("beat")}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := stream.Send(out); err != nil {
				fmt.Printf("remote connect lost\n")
				return err
			}
		}
	}
}


func (en *ExtentNode) ReplicateBlocks(ctx context.Context, req *pb.ReplicateBlocksRequest) (*pb.ReplicateBlocksResponse, error) {

	ex := en.getExtent((req.ExtentID))
	if ex == nil {
		return nil, errors.Errorf("no suck extent")
	}
	ex.Lock()
	defer ex.Unlock()

	if ex.HasLock(req.Revision) == false {
		return nil, errors.Errorf("lock by others")
	}

	if ex.CommitLength() != req.Commit {
		return nil, errors.Errorf("primary commitlength is different with replicates %d vs %d", req.Commit, ex.CommitLength())
	}
	ret, end, err := en.AppendWithWal(ex.Extent, req.Revision, req.Blocks)
	if err != nil {
		return nil, err
	}
	return &pb.ReplicateBlocksResponse{
		Code:    pb.Code_OK,
		Offsets: ret,
		End:     end,
	}, nil

}
//pool有可能是nil, 如果有nil存在返回error
func (en *ExtentNode) connPool(peers []string) ([]*conn.Pool, error) {
	var ret []*conn.Pool
	var err error
	for _, peer := range peers {
		pool := conn.GetPools().Connect(peer)
		if pool == nil {
			err = errors.Errorf("can not connected to %s", peer)
		}
		ret = append(ret, pool)
	}
	return ret, err
}

func (en *ExtentNode) validReq(extentID uint64, version uint64) (*extent.Extent, *pb.ExtentInfo, error) {
	
	ex := en.getExtent(extentID)
	if ex == nil {
		return nil, nil, errors.Errorf("no such extent %d on node %d", extentID, en.nodeID)
	}

	extentInfo := en.em.WaitVersion(extentID, version)
	if extentInfo == nil {
		return nil, nil, errors.Errorf("no such extent %d on etcd %d", extentID, en.nodeID)
	}
	
	if extentInfo.Eversion > version {
		return nil, nil, wire_errors.VersionLow
	}
	
	return ex.Extent, extentInfo, nil
}

func (en *ExtentNode) Append(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {

	errDone := func(err error) (*pb.AppendResponse, error) {
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.AppendResponse{
			Code:    code,
			CodeDes: desCode,
		}, nil
	}

	ex, extentInfo, err := en.validReq(req.ExtentID, req.Eversion)

	if err != nil {
		return errDone(err)
	}

	if extentInfo.Avali > 0 {
		return errDone(errors.Errorf("extent %d is sealed", req.ExtentID))
	}

	ex.Lock()
	defer ex.Unlock()

	if ex.HasLock(req.Revision) == false {
		return errDone(wire_errors.LockedByOther)
	}

	//extentInfo.Replicates : list of extent node'ID
	//extentInfo.Parity:

	//if any connection is lost, return error
	pools, err := en.connPool(en.em.GetPeers(req.ExtentID))
	if err != nil {
		return errDone(errors.Errorf("extent %d's append can not get pool[%s]", req.ExtentID, err.Error()))
	}

	//prepare data
	offset := ex.CommitLength()

	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	n := len(pools)

	dataBlocks := make([][]*pb.Block, n)
	//erasure code
	if len(extentInfo.Parity) > 0 {
		//EC, prepare data
		dataShard := len(extentInfo.Replicates)
		parityShard := len(extentInfo.Parity)

		for i := range req.Blocks {
			striped, err := erasure_code.ReedSolomon{}.Encode(req.Blocks[i].Data, dataShard, parityShard)
			if err != nil {
				return errDone(err)
			}
			for j := 0; j < len(striped); j++ {
				dataBlocks[j] = append(dataBlocks[j], &pb.Block{Data: striped[j]})
			}

		}
		//replicate code
	} else {
		for i := 0; i < len(dataBlocks); i++ {
			dataBlocks[i] = req.Blocks
		}
	}

	//FIXME: put stopper into sync.Pool
	stopper := utils.NewStopper()
	type Result struct {
		Error   error
		Offsets []uint32
		End     uint32
	}
	retChan := make(chan Result, n)

	//primary
	stopper.RunWorker(func() {
		//ret, err := ex.AppendBlocks(req.Blocks, &offset)
		ret, end, err := en.AppendWithWal(ex, req.Revision, dataBlocks[0])

		if ret != nil {
			retChan <- Result{Error: err, Offsets: ret, End: end}
		} else {
			retChan <- Result{Error: err}
		}
		xlog.Logger.Debugf("write primary done: %v, %v", ret, err)
	})

	//secondary
	for i := 1; i < n; i++ {
		j := i
		stopper.RunWorker(func() {
			conn := pools[j].Get()
			client := pb.NewExtentServiceClient(conn)
			res, err := client.ReplicateBlocks(pctx, &pb.ReplicateBlocksRequest{
				ExtentID: req.ExtentID,
				Commit:   offset,
				Blocks:   dataBlocks[j],
				Revision: req.Revision,
			})
			if res != nil {
				retChan <- Result{Error: err, Offsets: res.Offsets, End: res.End}
			} else {
				retChan <- Result{Error: err}
			}
			xlog.Logger.Debugf("write seconary done %v", err)

		})
	}

	stopper.Wait()
	close(retChan)

	var preOffsets []uint32
	preEnd := int64(-1)
	for result := range retChan {
		if preOffsets == nil {
			preOffsets = result.Offsets
		}
		if preEnd == -1 {
			preEnd = int64(result.End)
		}
		if result.Error != nil {
			return nil, result.Error
		}
		if !utils.EqualUint32(result.Offsets, preOffsets) || preEnd != int64(result.End) {
			return nil, errors.Errorf("block is not appended at the same offset [%v] vs [%v], end [%v] vs [%v]",
				result.Offsets, preOffsets, preEnd, result.End)
		}
	}

	return &pb.AppendResponse{
		Code:    pb.Code_OK,
		Offsets: preOffsets,
		End:     uint32(preEnd),
	}, nil
}

func (en *ExtentNode) SmartReadBlocks(ctx context.Context, req *pb.ReadBlocksRequest) (*pb.ReadBlocksResponse, error) {

	errDone := func(err error) (*pb.ReadBlocksResponse, error) {
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.ReadBlocksResponse{
			Code:    code,
			CodeDes: desCode,
		}, nil
	}

	xlog.Logger.Debugf("SmartRead of extentID %d, offset %d", req.ExtentID, req.Offset)
	//FIXME: local read
	_, exInfo, err := en.validReq(req.ExtentID, req.Eversion)
	if err != nil {
		return errDone(err)
	}

	//replicate read
	if len(exInfo.Parity) == 0 {
		return en.ReadBlocks(ctx, req)
	}

	//ereasure read
	dataShards := len(exInfo.Replicates)
	parityShards := len(exInfo.Parity)
	n := dataShards + parityShards

	//if we read from n > dataShard, call cancel() to stop
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)

	dataBlocks := make([][]*pb.Block, n)

	//channel
	type Result struct {
		Error error
		End   uint32
		Len   int
		Offsets []uint32
	}

	stopper := utils.NewStopper()
	retChan := make(chan Result, n)
	errChan := make(chan Result, n)

	submitReq := func(pool * conn.Pool, pos int) {
		if pool == nil {
			errChan <- Result{
				Error: errors.New("can not get conn"),
			}
			return
		}
		var res * pb.ReadBlocksResponse
		var err error
		
		if pos < len(exInfo.Replicates) && exInfo.Replicates[pos] == en.nodeID {
			res, err = en.ReadBlocks(pctx, req)
		} else {
			c := pb.NewExtentServiceClient(pool.Get())
			res, err = c.ReadBlocks(pctx, req)
		}
		if err != nil { //network error or disk error
			errChan <- Result{
				Error: err,}
			return
		}
		err = wire_errors.FromPBCode(res.Code, res.CodeDes)
		if err != nil && err != context.Canceled && err != wire_errors.EndOfExtent{
			errChan <- Result{
				Error: err,
			}
			return
		}

		//successful read
		//如果读到最后, err有可能是EndOfExtent
		dataBlocks[pos] = res.Blocks
		retChan <- Result{
			Error: err,
			End:   res.End,
			Len:   len(res.Blocks),
			Offsets: res.Offsets,
		}
	}

	//pools里面有可能有nil
	pools, _ := en.connPool(en.em.GetPeers(req.ExtentID))


	//only read from avali data
	//read data shards
	for i := 0; i < dataShards; i++ {
		j := i
		if exInfo.Avali > 0 &&  (1 << j) & exInfo.Avali == 0 {
			continue
		}
		stopper.RunWorker(func() {
			submitReq(pools[j], j)
		})
	}


	//read parity data
	for i := 0; i < parityShards; i++ {
		j := i
		if exInfo.Avali > 0 && (1 << (j + dataShards)) & exInfo.Avali == 0 {
			continue
		}
		stopper.RunWorker(func() {
			select {
			case <-stopper.ShouldStop():
				return
			case <-time.After(10 * time.Millisecond):
				submitReq(pools[dataShards+j], j+dataShards)
			}
		})

	}

	successRet := make([]Result, 0, dataShards)
	var success bool
waitResult:
	for {
		select {
		case <-pctx.Done(): //time out
			break waitResult
		case r := <-retChan:
			successRet = append(successRet, r)
			if len(successRet) == dataShards {
				success = true
				break waitResult
			}
	
		}
	}

	cancel()
	stopper.Stop()
	close(retChan)
	close(errChan)

	var lenOfBlocks int
	var end uint32
	var finalErr error
	var offsets []uint32
	if success {
		//collect successRet, END/ERROR should be the saved
		for i := 0; i < dataShards-1; i++ {
			//if err is EndExtent or EndStream, err must be the save
			if successRet[i].End != successRet[i+1].End {
				return errDone(errors.Errorf("extent %d: wrong successRet, %+v != %+v ", exInfo.ExtentID, successRet[i], successRet[i+1]))
			}
		}
		lenOfBlocks = successRet[0].Len
		end = successRet[0].End
		finalErr = successRet[0].Error
		offsets = successRet[0].Offsets
	} else {
		//collect errChan, find err
		errBuf := new(bytes.Buffer)
		for r := range errChan {
			errBuf.WriteString(r.Error.Error())
			errBuf.WriteString("\n")
		}
		return errDone(errors.New(errBuf.String()))
	}

	//join each block
	retBlocks := make([]*pb.Block, lenOfBlocks)
	for i := 0; i < lenOfBlocks; i++ {
		data := make([][]byte, n)
		for j := range dataBlocks {
			//in EC read, dataBlocks[j] could be nil, because we have got enough data to decode
			if dataBlocks[j] != nil {
				data[j] = dataBlocks[j][i].Data
			}
		}
		output, err := erasure_code.ReedSolomon{}.Decode(data, dataShards, parityShards)
		if err != nil {
			return errDone(err)
		}
		retBlocks[i] = &pb.Block{output}
	}

	code, _ := wire_errors.ConvertToPBCode(finalErr)
	return &pb.ReadBlocksResponse{
		Code:   code,
		Blocks: retBlocks,
		End:    end,
		Offsets: offsets,

	}, nil
}

func (en *ExtentNode) 	ReadBlocks(ctx context.Context, req *pb.ReadBlocksRequest) (*pb.ReadBlocksResponse, error) {

	errDone := func(err error) (*pb.ReadBlocksResponse, error) {
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.ReadBlocksResponse{
			Code:    code,
			CodeDes: desCode,
		}, nil
	}

	ex := en.getExtent(req.ExtentID)
	if ex == nil {
		return nil, errors.Errorf("node %d have no such extent :%d", en.nodeID, req.ExtentID)
	}
	blocks, offsets, end, err := ex.ReadBlocks(req.Offset, req.NumOfBlocks, (128 << 20))
	if err != nil && err != wire_errors.EndOfStream && err != wire_errors.EndOfExtent {
		return errDone(err)
	}

	xlog.Logger.Debugf("request extentID: %d, offset: %d, numOfBlocks: %d, response len(%d), %v ", req.ExtentID, req.Offset, req.NumOfBlocks,
		len(blocks), err)

	code, desCode := wire_errors.ConvertToPBCode(err)
	return &pb.ReadBlocksResponse{
		Code:    code,
		CodeDes: desCode,
		Blocks:  blocks,
		End:     end,
		Offsets: offsets,
	}, nil
}


func (en *ExtentNode) chooseDisktoAlloc() uint64 {
	//Other policies choose disk
	var i uint64 //diskID
	//only choose the first disk
	for _, disk := range en.diskFSs {
		if disk.Online() == 0 {
			continue
		}
		i = disk.diskID
		break
	}
	return i
}

func (en *ExtentNode) AllocExtent(ctx context.Context, req *pb.AllocExtentRequest) (*pb.AllocExtentResponse, error) {

	i := en.chooseDisktoAlloc()
	if i == 0 {
		xlog.Logger.Warnf("can not alloc extent %d", req.ExtentID)
		return nil, errors.Errorf("can not alloc extent %d", req.ExtentID)
	}


	ex, err := en.diskFSs[i].AllocExtent(req.ExtentID)
	if err != nil {
		xlog.Logger.Warnf("can not alloc extent %d, [%s]", req.ExtentID, err.Error())
		return nil, err
	}

	en.setExtent(req.ExtentID, &ExtentOnDisk{
		Extent: ex,
		diskID: 10,
	})

	return &pb.AllocExtentResponse{
		Code: pb.Code_OK,
		DiskID: en.diskFSs[i].diskID,
	}, nil
}

/*
func (en *ExtentNode) Seal(ctx context.Context, req *pb.SealRequest) (*pb.SealResponse, error) {
	ex := en.getExtent(req.ExtentID)
	if ex == nil {
		return nil, errors.Errorf("do not have extent %d, can not alloc new", req.ExtentID)
	}
	ex.Lock()
	defer ex.Unlock()

	if ex.HasLock(req.Revision) == false {
		return nil, errors.Errorf("lock by others")
	}
	
	err := ex.Seal(req.CommitLength)
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return nil, err
	}
	return &pb.SealResponse{Code: pb.Code_OK}, nil

}
*/



func (en *ExtentNode) CommitLength(ctx context.Context, req *pb.CommitLengthRequest) (*pb.CommitLengthResponse, error) {
	ex := en.getExtent(req.ExtentID)
	if ex == nil {
		return nil, errors.Errorf("do not have extent %d, can not alloc new", req.ExtentID)
	}

	l := ex.CommitLength()
	return &pb.CommitLengthResponse{
		Code:   pb.Code_OK,
		Length: l,
	}, nil

}


func (en *ExtentNode) Df(ctx context.Context, req *pb.DfRequest) (*pb.DfResponse, error) {
/*
	errDone := func(err error) (*pb.DfResponse, error) {
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.DfResponse{
			Code:    code,
			CodeDes: desCode,
		}, nil
	}
*/

	dfStatus := make(map[uint64]*pb.DF)
	for _, diskID := range req.DiskIDs {
		disk, ok := en.diskFSs[diskID]
		if ok {
			total, free, err := disk.Df()
			if err != nil {
				dfStatus[diskID] = &pb.DF{
					0,0,0,
				}
				continue
			}
			dfStatus[diskID] = &pb.DF{
				Total: total,
				Free: free,
				Online: disk.Online(),
			}
		} else {//no such disk
			dfStatus[diskID] = &pb.DF{0,0,0}
		}
	}


	var doneTasks []*pb.RecoveryTask
	for _, task := range req.Tasks {
		eod := en.getExtent(task.ExtentID)
		if eod == nil {
			continue
		}
		//recovery task is done
		doneTasks = append(doneTasks, &pb.RecoveryTask{	
			ExtentID: task.ExtentID,
			ReplaceID: task.ReplaceID,
			NodeID: en.nodeID,
			ReadyDiskID: eod.diskID,
		})
	}
	

	return &pb.DfResponse{
		Code: pb.Code_OK,
		DiskStatus: dfStatus,
		DoneTask: doneTasks,
	}, nil
}

func (en *ExtentNode) ReadEntries(ctx context.Context, req *pb.ReadEntriesRequest) (*pb.ReadEntriesResponse, error) {

	
	errDone := func(err error) (*pb.ReadEntriesResponse, error) {
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.ReadEntriesResponse{
			Code:    code,
			CodeDes: desCode,
		}, nil
	}
	
	ex := en.getExtent(req.ExtentID)
	if ex == nil {
		return errDone(errors.Errorf("no such extent %d", req.ExtentID))
	}

	replay := false
	if req.Replay > 0 {
		replay = true
	}
	res, _ := en.SmartReadBlocks(ctx, &pb.ReadBlocksRequest{
		ExtentID: req.ExtentID,
		Offset: req.Offset,
		NumOfBlocks: 20000,
		Eversion: req.Eversion,
	})


	if res.Code != pb.Code_OK && res.Code != pb.Code_EndOfExtent {
		return &pb.ReadEntriesResponse{
			Code:    res.Code,
			CodeDes: res.CodeDes,
		}, nil
	}

	//utils.AssertTrue(len(res.Blocks)>0)

	entries := make([]*pb.EntryInfo, len(res.Blocks))
	for i := 0 ; i < len(res.Blocks); i ++ {
		entry, err := extent.ExtractEntryInfo(res.Blocks[i], ex.ID, res.Offsets[i], replay)
		if err != nil {
			xlog.Logger.Errorf("unmarshal entries error %v\n", err)
			continue
		}
		//fmt.Printf("Read entries %s\n", string(entry.Log.Key))
		if i == len(res.Blocks)-1 {
			entry.End = res.End
		} else {
			entry.End = res.Offsets[i+1]
		}
		entries[i] = entry
	}

	return &pb.ReadEntriesResponse{
		Code: res.Code,
		Entries: entries,
		End : res.End,
	}, nil
}
