package smclient

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

//FIXME, 统一error的形式,
//pb.code表示逻辑错误
//err表示网络错误

var (
	errTruncateNoMatch = errors.New("truncateNoMatch")
)

type SMClient struct {
	conns []*grpc.ClientConn //protected by RWMUTEX
	sync.RWMutex
	lastLeader int32 //protected by atomic
	addrs      []string
	nodesLock  sync.RWMutex
	nodes      map[uint64]*pb.NodeInfo //update nodes info
}

func NewSMClient(addrs []string) *SMClient {
	utils.AssertTrue(xlog.Logger != nil)
	return &SMClient{
		addrs: addrs,
	}
}

func (client *SMClient) LookupNode(nodeID uint64) *pb.NodeInfo {
	client.nodesLock.RLock()
	info := client.nodes[nodeID]
	client.nodesLock.RUnlock()
	if info != nil {
		return info
	}

	//try to update nodes
	client.updateNodes()

	client.nodesLock.RLock()
	info = client.nodes[nodeID]
	client.nodesLock.RUnlock()
	return info
}

func (client *SMClient) updateNodes() {
	x, err := client.NodesInfo(context.Background())
	if err != nil {
		xlog.Logger.Warn("failed to call update nodes")
		return
	}
	client.nodesLock.Lock()
	defer client.nodesLock.Unlock()
	client.nodes = x
}

// Connect to Zero's grpc service, if all of connect failed to connnect, return error
func (client *SMClient) Connect() error {

	if len(client.addrs) == 0 {
		return errors.Errorf("addr is nil")
	}
	client.Lock()
	client.conns = nil
	errCount := 0
	for _, addr := range client.addrs {
		c, err := grpc.Dial(addr, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
		if err != nil {
			errCount++
		}
		client.conns = append(client.conns, c)
	}
	client.Unlock()
	if errCount == len(client.addrs) {
		return errors.Errorf("all connection failed")
	}
	atomic.StoreInt32(&client.lastLeader, 0)

	//update nodes periodically
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		for {
			select {
			case <-ticker.C:
				client.updateNodes()
			}
		}
	}()
	return nil
}

func (client *SMClient) CurrentLeader() string {
	current := atomic.LoadInt32(&client.lastLeader)
	return client.addrs[current]
}

func (client *SMClient) Close() {
	client.Lock()
	defer client.Unlock()
	for _, c := range client.conns {
		c.Close()
	}
	client.conns = nil
}

func (client *SMClient) Alive() bool {
	client.RLock()
	defer client.RUnlock()
	return len(client.conns) > 0
}

func (client *SMClient) RegisterNode(ctx context.Context, addr string) (uint64, error) {
	client.RLock()
	defer client.RUnlock()
	current := atomic.LoadInt32(&client.lastLeader)
	for loop := 0; loop < len(client.conns)*2; loop++ {
		if client.conns != nil && client.conns[current] != nil {
			c := pb.NewStreamManagerServiceClient(client.conns[current])
			res, err := c.RegisterNode(ctx, &pb.RegisterNodeRequest{
				Addr: addr,
			})
			if err == context.Canceled || err == context.DeadlineExceeded {
				return 0, err
			}
			if err != nil {
				xlog.Logger.Warnf(err.Error())
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(500 * time.Millisecond)
				continue
			}
			atomic.StoreInt32(&client.lastLeader, current)
			return res.NodeId, nil
		}
	}
	return 0, errors.Errorf("timeout : cannot register Node")
}

func (client *SMClient) CreateStream(ctx context.Context) (*pb.StreamInfo, *pb.ExtentInfo, error) {
	client.RLock()
	defer client.RUnlock()
	current := atomic.LoadInt32(&client.lastLeader)
	for loop := 0; loop < len(client.conns)*2; loop++ {
		if client.conns != nil && client.conns[current] != nil {
			c := pb.NewStreamManagerServiceClient(client.conns[current])
			res, err := c.CreateStream(ctx, &pb.CreateStreamRequest{})
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil, nil, err
			}
			if err != nil {
				xlog.Logger.Warnf(err.Error())
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(500 * time.Millisecond)
				continue
			}
			atomic.StoreInt32(&client.lastLeader, current)
			return res.Stream, res.Extent, nil

		}
	}
	return nil, nil, errors.Errorf("timeout: CreateStream failed")
}

func (client *SMClient) StreamAllocExtent(ctx context.Context, streamID uint64, extentToSeal uint64) (*pb.ExtentInfo, error) {
	client.RLock()
	defer client.RUnlock()
	last := atomic.LoadInt32(&client.lastLeader)
	current := last
	for loop := 0; loop < len(client.conns)*2; loop++ {
		if client.conns != nil && client.conns[current] != nil {
			c := pb.NewStreamManagerServiceClient(client.conns[current])
			//
			res, err := c.StreamAllocExtent(ctx, &pb.StreamAllocExtentRequest{
				StreamID:     streamID,
				ExtentToSeal: extentToSeal,
			})
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil, err
			}
			if err != nil {
				xlog.Logger.Warnf(err.Error())
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(500 * time.Millisecond)
				continue
			}
			if current != last {
				atomic.StoreInt32(&client.lastLeader, current)
			}
			return res.Extent, nil
		}
	}
	return nil, errors.Errorf("timeout: CreateStream failed")
}

func (client *SMClient) NodesInfo(ctx context.Context) (map[uint64]*pb.NodeInfo, error) {
	client.RLock()
	defer client.RUnlock()
	last := atomic.LoadInt32(&client.lastLeader)
	current := last
	for loop := 0; loop < len(client.conns)*2; loop++ {
		if client.conns != nil && client.conns[current] != nil {
			c := pb.NewStreamManagerServiceClient(client.conns[current])
			//
			res, err := c.NodesInfo(ctx, &pb.NodesInfoRequest{})
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil, err
			}
			if err != nil {
				xlog.Logger.Warnf(err.Error())
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(500 * time.Millisecond)
				continue
			}
			if current != last {
				atomic.StoreInt32(&client.lastLeader, current)
			}
			return res.Nodes, nil
		}
	}
	return nil, errors.Errorf("timeout: NodesInfo failed")
}

func (client *SMClient) ExtentInfo(ctx context.Context, extentIDs []uint64) (map[uint64]*pb.ExtentInfo, error) {
	client.RLock()
	defer client.RUnlock()
	last := atomic.LoadInt32(&client.lastLeader)
	current := last
	for loop := 0; loop < len(client.conns)*2; loop++ {
		if client.conns != nil && client.conns[current] != nil {
			c := pb.NewStreamManagerServiceClient(client.conns[current])
			//
			res, err := c.ExtentInfo(ctx, &pb.ExtentInfoRequest{
				Extents: extentIDs,
			})
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil, err
			}
			if err != nil {
				xlog.Logger.Warnf(err.Error())
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(500 * time.Millisecond)
				continue
			}
			if current != last {
				atomic.StoreInt32(&client.lastLeader, current)
			}
			return res.Extents, nil
		}
	}
	return nil, errors.Errorf("timeout: ExtentInfo failed")
}

func (client *SMClient) StreamInfo(ctx context.Context, streamIDs []uint64) (map[uint64]*pb.StreamInfo, map[uint64]*pb.ExtentInfo, error) {
	client.RLock()
	defer client.RUnlock()
	last := atomic.LoadInt32(&client.lastLeader)
	current := last
	for loop := 0; loop < len(client.conns)*2; loop++ {
		if client.conns != nil && client.conns[current] != nil {
			c := pb.NewStreamManagerServiceClient(client.conns[current])
			//
			res, err := c.StreamInfo(ctx, &pb.StreamInfoRequest{
				StreamIDs: streamIDs,
			})
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil, nil, err
			}
			if err != nil {
				xlog.Logger.Warnf(err.Error())
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(500 * time.Millisecond)
				continue
			}
			if current != last {
				atomic.StoreInt32(&client.lastLeader, current)
			}
			return res.Streams, res.Extents, nil
		}
	}
	return nil, nil, errors.Errorf("timeout: StreamInfo failed")
}

func (client *SMClient) TruncateStream(ctx context.Context, streamID uint64, extentIDs []uint64) error {
	client.RLock()
	defer client.RUnlock()
	last := atomic.LoadInt32(&client.lastLeader)
	current := last
	for loop := 0; loop < len(client.conns)*2; loop++ {
		if client.conns != nil && client.conns[current] != nil {
			c := pb.NewStreamManagerServiceClient(client.conns[current])
			res, err := c.Truncate(ctx, &pb.TruncateRequest{
				StreamID:  streamID,
				ExtentIDs: extentIDs,
			})
			if err == context.Canceled || err == context.DeadlineExceeded {
				return err
			}
			if err != nil {
				xlog.Logger.Warnf(err.Error())
				current = (current + 1) % int32(len(client.conns))
				time.Sleep(500 * time.Millisecond)
				continue
			}
			switch res.Code {
			case pb.Code_OK:
				break
			case pb.Code_TruncateNotMatch:
				return errTruncateNoMatch
			default:
				return errors.New(res.Code.String())
			}

			if current != last {
				atomic.StoreInt32(&client.lastLeader, current)
			}
			return nil
		}
	}
	return errors.Errorf("timeout: StreamInfo failed")

}
