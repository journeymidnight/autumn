package streammanager

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

/*
StreamInfo(context.Context, *StreamInfoRequest) (*StreamInfoResponse, error)
ExtentInfo(context.Context, *ExtentInfoRequest) (*ExtentInfoResponse, error)
NodesInfo(context.Context, *NodesInfoRequest) (*NodesInfoResponse, error)
StreamAllocExtent(context.Context, *StreamAllocExtentRequest) (*StreamAllocExtentResponse, error)
CreateStream(context.Context, *CreateStreamRequest) (*CreateStreamResponse, error)
RegisterNode(context.Context, *RegisterNodeRequest) (*RegisterNodeResponse, error)
*/

func (sm *StreamManager) CreateStream(ctx context.Context, req *pb.CreateStreamRequest) (*CreateStreamResponse, error) {
	if !sm.AmLeader() {
		return nil, errors.Errorf("not a leader")
	}
	//block forever
	start, _ := sm.allocUniqID(2)
	//streamID and extentID.
	streamID := start
	extentID := start + 1
	var nodes []NodeStatus
	//FIXME:lock
	for _, n := range sm.nodes {
		nodes = append(nodes, *n)
	}

	nodes, err := sm.policy.AllocExtent(nodes, 3, nil)
	if err != nil {
		return nil, err
	}

	pctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	stopper := utils.NewStopper()

	var complets int32
	for _, node := range nodes {
		addr := node.addr
		stopper.RunWorker(func() {
			pool := conn.GetPools().Connect(addr)
			conn := pool.Get()
			c := pb.NewInternalExtentServiceClient(conn)
			_, err := c.AllocExtent(pctx, &pb.AllocExtentRequest{
				ExtentID: extentID,
			})
			if err != nil {
				xlog.Logger.Warnf(err.Error())
				return
			}
			atomic.AddInt32(&complets, 1)
		})
	}
	stopper.Wait()

	if complets != 3 || !sm.AmLeader() {
		return nil, errors.Errorf("not to create stream")
	}

	//修改ETCD
	//修改streams
	streamKey := formatStreamKey(streamID)
	streamValue := pb.OrderedExtentIDs{
		Extents: []uint64{extentID},
	}
	sdata, err := streamValue.Marshal()
	utils.Check(err)
	//修改extents
	extentKey := formatExtentReplicate(extentID)
	extentsValue := pb.OrderedNodesReplicates{
		Nodes: extractNodeId(nodes),
	}
	edata, err := extentsValue.Marshal()
	utils.Check(err)

	ops := []clientv3.Op{
		clientv3.OpPut(streamKey, string(sdata)),
		clientv3.OpPut(extentKey, string(edata)),
	}

	err = manager.EtctSetKVS(sm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
	}, ops)
	//
	if err != nil {
		return nil, err
	}

	return pb.CreateStreamResponse{
		Code:               pb.Code_OK,
		StreamID:           streamID,
		ExtentID:           extentID,
		ExtentIDreplicates: &extentsValue,
	}, nil

}

func (sm *StreamManager) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {

}

func extractNodeId(nodes []NodeStatus) []uint64 {
	var ret []uint64
	for _, node := range nodes {
		ret = append(ret, node.ID)
	}
	return ret
}

func formatStreamKey(ID uint64) string {
	return fmt.Sprintf("streams/%d", ID)
}

func formatNodeKey(ID uint64) string {
	return fmt.Sprintf("nodes/%d", ID)
}

func formatExtentReplicate(ID uint64) string {
	return fmt.Sprintf("extents/%d", ID)
}
