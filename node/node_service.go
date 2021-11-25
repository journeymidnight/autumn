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

	errDone := func(err error) (*pb.ReplicateBlocksResponse, error) {
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.ReplicateBlocksResponse{
			Code:    code,
			CodeDes: desCode,
		}, nil
	}

	ex := en.getExtent((req.ExtentID))
	if ex == nil {
		return errDone(errors.Errorf("node %d have no such extent :%d", en.nodeID, req.ExtentID))
	}
	ex.Lock()
	defer ex.Unlock()

	if !ex.HasLock(req.Revision) {
		return errDone(wire_errors.LockedByOther)
	}

	if ex.CommitLength() < req.Commit {
		return errDone(errors.Errorf("primary commitlength is different with replicates %d vs %d", req.Commit, ex.CommitLength()))
	}

	if ex.CommitLength() > req.Commit {
		ex.Truncate(req.Commit)
	}

	//ex.CommitLength() == req.Commit
	utils.AssertTrue(ex.CommitLength() == req.Commit)

	ret, end, err := en.AppendWithWal(ex.Extent, req.Revision, req.Blocks, req.MustSync)
	if err != nil {
		return errDone(err)
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

	/*
		startTime := time.Now()
		defer func() {
			fmt.Printf("node %d Append extent %d, duration is %+v\n", en.nodeID, req.ExtentID, time.Since(startTime))
		}()
	*/

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

	if !ex.HasLock(req.Revision) {
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
		ret, end, err := en.AppendWithWal(ex, req.Revision, dataBlocks[0], req.MustSync)

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
				MustSync: req.MustSync,
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
			fmt.Printf("append on extent %d error: %v\n", req.ExtentID, result.Error)
			ex.Truncate(offset) //reset local offset
			return nil, result.Error
		}
		if !utils.EqualUint32(result.Offsets, preOffsets) || preEnd != int64(result.End) {
			ex.Truncate(offset) //reset local offset
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

func (en *ExtentNode) ReadBlocks(ctx context.Context, req *pb.ReadBlocksRequest) (*pb.ReadBlocksResponse, error) {

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

	var blocks []*pb.Block
	var end uint32
	var err error
	var offsets []uint32

	if req.OnlyLastBlock {
		blocks, offsets, end, err = ex.ReadLastBlock()
	} else {
		blocks, offsets, end, err = ex.ReadBlocks(req.Offset, req.NumOfBlocks, (32 << 20))
	}

	if err != nil && err != wire_errors.EndOfExtent {
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
	//only choose the first disk
	for _, disk := range en.diskFSs {
		if disk.Online() {
			return disk.diskID
		}
	}
	return 0
}

func (en *ExtentNode) AllocExtent(ctx context.Context, req *pb.AllocExtentRequest) (*pb.AllocExtentResponse, error) {

	errDone := func(err error) (*pb.AllocExtentResponse, error) {
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.AllocExtentResponse{
			Code:    code,
			CodeDes: desCode,
		}, nil
	}

	i := en.chooseDisktoAlloc()
	if i == 0 {
		xlog.Logger.Warnf("can not alloc extent %d", req.ExtentID)
		return errDone(errors.Errorf("can not alloc extent %d", req.ExtentID))
	}

	ex, err := en.diskFSs[i].AllocExtent(req.ExtentID)
	if err != nil {
		xlog.Logger.Warnf("can not alloc extent %d, [%s]", req.ExtentID, err.Error())
		return errDone(err)
	}

	en.setExtent(req.ExtentID, &ExtentOnDisk{
		Extent: ex,
		diskID: en.diskFSs[i].diskID,
	})

	return &pb.AllocExtentResponse{
		Code:   pb.Code_OK,
		DiskID: en.diskFSs[i].diskID,
	}, nil
}

func (en *ExtentNode) CommitLength(ctx context.Context, req *pb.CommitLengthRequest) (*pb.CommitLengthResponse, error) {

	errDone := func(err error) (*pb.CommitLengthResponse, error) {
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.CommitLengthResponse{
			Code:    code,
			CodeDes: desCode,
		}, nil
	}

	ex := en.getExtent(req.ExtentID)
	if ex == nil {
		return errDone(errors.Errorf("do not have extent %d, can not alloc new", req.ExtentID))
	}

	if req.Revision > 0 {
		if !ex.HasLock(req.Revision) {
			return errDone(wire_errors.LockedByOther)
		}
	}

	l := ex.CommitLength()
	return &pb.CommitLengthResponse{
		Code:   pb.Code_OK,
		Length: l,
	}, nil
}

func (en *ExtentNode) Df(ctx context.Context, req *pb.DfRequest) (*pb.DfResponse, error) {
	dfStatus := make(map[uint64]*pb.DF)
	for _, diskID := range req.DiskIDs {
		disk, ok := en.diskFSs[diskID]
		if ok {
			total, free, err := disk.Df()
			if err != nil {
				dfStatus[diskID] = &pb.DF{
					0, 0, false,
				}
				continue
			}
			dfStatus[diskID] = &pb.DF{
				Total:  total,
				Free:   free,
				Online: disk.Online(),
			}
		} else { //no such disk
			dfStatus[diskID] = &pb.DF{0, 0, false}
		}
	}

	var doneTasks []*pb.RecoveryTaskStatus
	for _, task := range req.Tasks {
		eod := en.getExtent(task.ExtentID)
		if eod == nil {
			continue
		}
		//recovery task is done
		doneTasks = append(doneTasks, &pb.RecoveryTaskStatus{
			Task: &pb.RecoveryTask{
				ExtentID:  task.ExtentID,
				ReplaceID: task.ReplaceID,
				NodeID:    en.nodeID,
			},
			ReadyDiskID: eod.diskID,
		})
	}

	return &pb.DfResponse{
		DiskStatus: dfStatus,
		DoneTask:   doneTasks,
	}, nil
}
