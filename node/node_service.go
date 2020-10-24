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
	"path"
	"time"

	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
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
	ret, err := ex.AppendBlocks(req.Blocks, req.Commit)
	if err != nil {
		return nil, err
	}
	return &pb.ReplicateBlocksResponse{
		Code:    pb.Code_OK,
		Offsets: ret,
	}, nil

}

func (en *ExtentNode) connPoolOfReplicates(peers []string) ([]*conn.Pool, error) {
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

//一般来说,需要用slurp的方式合并IO, 但是考虑到stream上层是由一单线程, io queue在partiion layer实现
func (en *ExtentNode) Append(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	ex := en.getExtent(req.ExtentID)
	if ex == nil {
		xlog.Logger.Debugf("no extent %d", req.ExtentID)
		return nil, errors.Errorf("not such extent")
	}
	ex.Lock()
	defer ex.Unlock()

	pools, err := en.connPoolOfReplicates(req.Peers)
	if err != nil {
		return nil, err
	}
	offset := ex.CommitLength()

	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	//FIXME: put stopper into sync.Pool
	stopper := utils.NewStopper()

	type Result struct {
		Error   error
		Offsets []uint32
	}
	retChan := make(chan Result, 3)
	//primary
	stopper.RunWorker(func() {
		//start := time.Now()
		ret, err := ex.AppendBlocks(req.Blocks, offset)
		//fmt.Printf("len %d, %v\n", len(req.Blocks), time.Now().Sub(start))

		if ret != nil {
			retChan <- Result{Error: err, Offsets: ret}
		} else {
			retChan <- Result{Error: err}
		}
		xlog.Logger.Debugf("write primary done: %v", ret, err)
	})
	//secondary

	for i := 1; i < 3; i++ {
		j := i
		stopper.RunWorker(func() {
			conn := pools[j].Get()
			client := pb.NewExtentServiceClient(conn)
			res, err := client.ReplicateBlocks(pctx, &pb.ReplicateBlocksRequest{
				ExtentID: req.ExtentID,
				Commit:   offset,
				Blocks:   req.Blocks,
			})
			if res != nil {
				retChan <- Result{Error: err, Offsets: res.Offsets}
			} else {
				retChan <- Result{Error: err}
			}
			xlog.Logger.Debugf("write seconary done %v", err)

		})
	}

	stopper.Wait()
	close(retChan)
	var preOffsets []uint32
	for result := range retChan {
		if preOffsets == nil {
			preOffsets = result.Offsets
		}
		if result.Error != nil || !utils.EqualUint32(result.Offsets, preOffsets) {
			return nil, errors.Errorf("%v, %v vs pre: %v", result.Error, result.Offsets, preOffsets)
		}
	}
	return &pb.AppendResponse{
		Code:    pb.Code_OK,
		Offsets: preOffsets,
	}, nil
}

func (en *ExtentNode) ReadBlocks(ctx context.Context, req *pb.ReadBlocksRequest) (*pb.ReadBlocksResponse, error) {
	ex := en.getExtent(req.ExtentID)
	if ex == nil {
		return nil, errors.Errorf("no such extent")
	}
	blocks, err := ex.ReadBlocks(req.Offset, req.NumOfBlocks)
	if err != nil {
		return nil, err
	}
	return &pb.ReadBlocksResponse{
		Code:   pb.Code_OK,
		Blocks: blocks,
	}, nil
}

func formatExtentName(dir string, ID uint64) string {
	return path.Join(dir, fmt.Sprintf("extent_%d.ext", ID))
}

func (en *ExtentNode) AllocExtent(ctx context.Context, req *pb.AllocExtentRequest) (*pb.AllocExtentResponse, error) {
	ex := en.getExtent(req.ExtentID)
	if ex != nil {
		return nil, errors.Errorf("have extent, can not alloc new")
	}

	newEx, err := extent.CreateExtent(formatExtentName(en.baseFileDir, req.ExtentID), req.ExtentID)
	if err != nil {
		return nil, err
	}
	en.setExtent(newEx.ID, newEx)
	return &pb.AllocExtentResponse{
		Code: pb.Code_OK,
	}, nil
}

func (en *ExtentNode) Seal(ctx context.Context, req *pb.SealRequest) (*pb.SealResponse, error) {
	ex := en.getExtent(req.ExtentID)
	if ex != nil {
		return nil, errors.Errorf("have extent, can not alloc new")
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
	if ex != nil {
		return nil, errors.Errorf("have extent, can not alloc new")
	}

	l := ex.CommitLength()
	return &pb.CommitLengthResponse{
		Code:   pb.Code_OK,
		Length: l,
	}, nil

}
