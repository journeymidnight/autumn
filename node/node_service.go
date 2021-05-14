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
	"math/rand"
	"time"

	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/erasure_code"
	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var (
	_ = conn.GetPools
	_ = fmt.Printf
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
	if ex.CommitLength() != req.Commit {
		return nil, errors.Errorf("primary commitlength is different with replicates %d vs %d", req.Commit, ex.CommitLength())
	}
	ret, end, err := en.AppendWithWal(ex, req.Blocks)
	if err != nil {
		return nil, err
	}
	return &pb.ReplicateBlocksResponse{
		Code:    pb.Code_OK,
		Offsets: ret,
		End : end,
	}, nil

}

func (en *ExtentNode) connPool(peers []string) ([]*conn.Pool, error) {
	var ret []*conn.Pool
	for _, peer := range peers {
		pool := conn.GetPools().Connect(peer)
		if !pool.IsHealthy() {
			return nil, errors.Errorf("remote peer %s not healthy", peer)
		}
		ret = append(ret, pool)
	}
	return ret, nil
}

const (
	cellSize = 4<< 10 //4KB
)


func (en *ExtentNode) validReq(extentID uint64, version uint64) (*extent.Extent, *pb.ExtentInfo, error) {
	extentInfo := en.em.GetExtentInfo(extentID)
	if extentInfo == nil {
		return nil, nil, errors.Errorf("no such extent %d on etcd %d",extentID, en.nodeID)
	}

	ex := en.getExtent(extentID)
	if ex == nil {
		return nil, nil, errors.Errorf("no such extent %d on node %d",extentID, en.nodeID)
	}

	for i := 0 ; i < 3 && extentInfo.Eversion != version ; i ++ {
		if extentInfo.Eversion > version {
			return nil, nil, wire_errors.VersionLow
		} else if extentInfo.Eversion < version {
			extentInfo = en.em.Update(extentID)
		}
	}

	if extentInfo.Eversion != version {
		return nil, nil, errors.New("i tried 3 times to match version to you, buf failed. network partition?")
	}

	if extentInfo.IsSealed > 0 {
		return nil, nil, errors.New("extent is sealed")
	}

	return ex, extentInfo, nil
}


func (en *ExtentNode) Append(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
		
	errDone := func(err error) (*pb.AppendResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.AppendResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}


	ex, extentInfo, err := en.validReq(req.ExtentID, req.Eversion)

	if err != nil {
		return errDone(err)
	}


	ex.Lock()
	defer ex.Unlock()
	
	//extentInfo.Replicates : list of extent node'ID
	//extentInfo.Parity:

	
	pools, err := en.connPool(en.em.GetPeers(req.ExtentID))
	if err != nil {
		return errDone(err)
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
			striped, err := erasure_code.ReedSolomon{}.Encode(req.Blocks[i].Data, uint32(dataShard),  uint32(parityShard),  cellSize)
			if err != nil {
				return errDone(err)
			}
			for j := 0 ; j < len(striped) ; j ++ {
				dataBlocks[j] = append(dataBlocks[j], &pb.Block{Data:striped[j]})
			}

		}	
	//replicate code	
	} else {
		for i := 0; i < len(dataBlocks) ; i ++ {
			dataBlocks[i] = req.Blocks
		}
	}

	//FIXME: put stopper into sync.Pool
	stopper := utils.NewStopper()
	type Result struct {
		Error   error
		Offsets []uint32
		End uint32
	}
	retChan := make(chan Result, n)

	//primary
	stopper.RunWorker(func() {
		//ret, err := ex.AppendBlocks(req.Blocks, &offset)
		ret, end, err := en.AppendWithWal(ex, dataBlocks[0])

		if ret != nil {
			retChan <- Result{Error: err, Offsets: ret, End:end}
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
	preEnd  := int64(-1)
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
		if !utils.EqualUint32(result.Offsets, preOffsets) || preEnd != int64(result.End){
			return nil, errors.Errorf("block is not appended at the same offset [%v] vs [%v], end [%v] vs [%v]", 
			result.Offsets, preOffsets, preEnd, result.End)
		}
	}

	return &pb.AppendResponse{
		Code:    pb.Code_OK,
		Offsets: preOffsets,
		End: uint32(preEnd),
	}, nil
}


func (en *ExtentNode) SmartReadBlocks(ctx context.Context, req *pb.ReadBlocksRequest) (*pb.ReadBlocksResponse, error) {

	errDone := func(err error) (*pb.ReadBlocksResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.ReadBlocksResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

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
	pctx, cancel := context.WithTimeout(ctx, 5 * time.Second)
	
	dataBlocks := make([][]*pb.Block, n)

	//channel
	type Result struct {
		Error   error
		End uint32
		Len int
	}

	stopper :=utils.NewStopper()
	retChan := make(chan Result, n)
	errChan := make(chan Result, n)

	submitReq := func(conn *grpc.ClientConn, pos int){
		if conn == nil {
			errChan <- Result{
				Error : errors.New("can not get conn"),
			}
			return
		}
		c := pb.NewExtentServiceClient(conn)
		res, err := c.ReadBlocks(pctx, req)
		err = wire_errors.FromPBCode(res.Code, res.CodeDes)
		if err != nil && err != context.Canceled && err != wire_errors.EndOfExtent && err != wire_errors.EndOfStream {
			errChan <- Result{
				Error: err,
			}
			return
		}

		//successful read
		//insert into res.Blocks
		dataBlocks[pos] = res.Blocks
		retChan <- Result{
			Error : err,
			End: res.End,
			Len: len(res.Blocks),
		}
	}

	pools, err := en.connPool(en.em.GetPeers(req.ExtentID))

	if err != nil {
		cancel()
		return errDone(err)
	}


	//read data shards
	for i := 0 ;i < dataShards; i ++ {
		j := i
		stopper.RunWorker(func(){
			conn := pools[j].Get()
			submitReq(conn, j)
		})
	}

	//read parity data
	for i := 0 ; i < parityShards; i ++ {
		j := i
		stopper.RunWorker(func(){
			conn := pools[dataShards+j].Get()
			select {
			case <- stopper.ShouldStop():
				return
			case <- time.After(100 * time.Millisecond):
				submitReq(conn, j + dataShards)
			}
		})

	}


	successRet := make([]Result, 0, dataShards)
	var success bool
	waitResult:
	for {
		select {
		case <- pctx.Done(): //time out
			break waitResult
		case r := <- retChan:
			successRet = append(successRet, r)
			if len(successRet) == dataShards {
				success = true
				break waitResult
			}
		}
	}

	cancel()
	stopper.Close()
	close(retChan)
	close(errChan)

	var lenOfBlocks int
	var end uint32
	var finalErr error
	if success {
		//collect successRet, END/ERROR should be the save
		for i := 0; i < dataShards -1 ; i ++ {
			//if err is EndExtent or EndStream, err must be the save
			if successRet[i] != successRet[i+1] {
				return errDone(errors.Errorf("wrong successRet, %v != %v ", successRet[i], successRet[i+1]))
			}
		}
		lenOfBlocks = successRet[0].Len
		end = successRet[0].End
		finalErr = successRet[0].Error
	} else {
		//FIXME collect errChan, find err
		r := <- errChan
		return errDone(r.Error)
	}

	//join each block
	retBlocks := make([]*pb.Block, lenOfBlocks)
	for i := 0 ;i < lenOfBlocks ; i ++ {
		data := make([][]byte, n)
		for j := range dataBlocks {
			//in EC read, dataBlocks[j] could be nil, because we have got enough data to decode
			if dataBlocks[j]!= nil {
				data[j] = dataBlocks[j][i].Data
			}
		}
		output, err := erasure_code.ReedSolomon{}.Decode(data, uint32(dataShards), uint32(parityShards), cellSize)
		if err != nil {
			return errDone(err)
		}
		retBlocks[i] = &pb.Block{output}
	}

	code, _ := wire_errors.ConvertToPBCode(finalErr)
	return &pb.ReadBlocksResponse{
		Code: code,
		Blocks: retBlocks,
		End: end,
	}, nil
}

func (en *ExtentNode) ReadBlocks(ctx context.Context, req *pb.ReadBlocksRequest) (*pb.ReadBlocksResponse, error) {

	errDone := func(err error) (*pb.ReadBlocksResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.ReadBlocksResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

	ex := en.getExtent(req.ExtentID)
	if ex == nil {
		return nil, errors.Errorf("no such extent")
	}
	blocks, _, end, err := ex.ReadBlocks(req.Offset, req.NumOfBlocks, (32 << 20))
	if err != nil && err != wire_errors.EndOfStream && err != wire_errors.EndOfExtent {
		return errDone(err)
	}

	xlog.Logger.Debugf("request extentID: %d, offset: %d, numOfBlocks: %d, response len(%d), %v ", req.ExtentID, req.Offset, req.NumOfBlocks,
		len(blocks), err)

	code, desCode := wire_errors.ConvertToPBCode(err)
	return &pb.ReadBlocksResponse{
		Code:   code,
		CodeDes: desCode,
		Blocks: blocks,
		End: end,
	}, nil
}


func (en *ExtentNode) AllocExtent(ctx context.Context, req *pb.AllocExtentRequest) (*pb.AllocExtentResponse, error) {
	//Other policies
	i := rand.Intn(len(en.diskFSs))
	ex, err := en.diskFSs[i].AllocExtent(req.ExtentID)
	if err != nil {
		xlog.Logger.Warnf("can not alloc extent %d, [%s]", req.ExtentID, err.Error())
		return nil, err
	}
	en.extentMap.Store(req.ExtentID, ex)

	return &pb.AllocExtentResponse{
		Code: pb.Code_OK,
	}, nil
}

func (en *ExtentNode) Seal(ctx context.Context, req *pb.SealRequest) (*pb.SealResponse, error) {
	ex := en.getExtent(req.ExtentID)
	if ex == nil {
		return nil, errors.Errorf("do not have extent %d, can not alloc new", req.ExtentID)
	}
	err := ex.Seal(req.CommitLength)
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return nil, err
	}
	return &pb.SealResponse{Code: pb.Code_OK}, nil

}

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

func (en *ExtentNode) RequireRecovery(ctx context.Context, req *pb.RequireRecoveryRequest) (*pb.RequireRecoveryResponse, error) {
	return nil, errors.New("not implemented")
}

func (en *ExtentNode) Df(ctx context.Context, req *pb.DfRequest) (*pb.DfResponse, error) {
	
	errDone := func(err error) (*pb.DfResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.DfResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

	totalSum := uint64(0)
	totalFree := uint64(0)
	for _, fs := range en.diskFSs {
		sum, free, err := fs.Df()
		if err != nil {
			return errDone(err)
		}
		totalSum += sum
		totalFree += free
	}
	return &pb.DfResponse{
		Code: pb.Code_OK,
		Df: &pb.DF{
			Total: totalSum,
			Free: totalFree,
		},
	},nil
}

func (en *ExtentNode) ReadEntries(ctx context.Context, req *pb.ReadEntriesRequest) (*pb.ReadEntriesResponse, error) {

	return nil, errors.New("to be implemented")
	/*
	ex := en.getExtent(req.ExtentID)
	if ex == nil {
		return nil, errors.Errorf("no such extent")
	}
	replay := false
	if req.Replay > 0 {
		replay = true
	}
	ei, end, err := ex.ReadEntries(req.Offset, (25 << 20), replay)
	if err != nil && err != wire_errors.EndOfExtent && err != wire_errors.EndOfExtent {
		xlog.Logger.Infof("request ReadEntires extentID: %d, offset: %d, : %v", req.ExtentID, req.Offset, err)
		return nil, err
	}

	return &pb.ReadEntriesResponse{
		Code:      errorToCode(err),
		Entries:   ei,
		End: end,
	}, nil
*/
}
