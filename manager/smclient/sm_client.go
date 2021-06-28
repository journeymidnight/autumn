package smclient

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
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
}

func NewSMClient(addrs []string) *SMClient {
	utils.AssertTrue(xlog.Logger != nil)
	return &SMClient{
		addrs: addrs,
	}
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


func (client *SMClient) try(f func(conn *grpc.ClientConn) bool, x time.Duration) {
	client.RLock()
	connLen := len(client.conns)
	utils.AssertTrue(connLen > 0)
	client.RUnlock()

	current := atomic.LoadInt32(&client.lastLeader)
	for loop := 0; loop < connLen * 4; loop++ {
		client.RLock()
		if client.conns != nil && client.conns[current] != nil {
			//if f() return true, sleep and continue
			//if f() return false, return
			if f(client.conns[current]) == true {
				current = (current + 1) % int32(connLen)
				client.RUnlock()
				time.Sleep(x)
				continue
			} else {
				atomic.StoreInt32(&client.lastLeader, current)
				client.RUnlock()
				return
			}
		}
		
	}
}

func (client *SMClient) RegisterNode(ctx context.Context, addr string) (uint64, error) {
	err := errors.New("can not find connection to stream manager")
	var res *pb.RegisterNodeResponse
	nodeID := uint64(0)
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.RegisterNode(ctx, &pb.RegisterNodeRequest{
			Addr: addr,
		})
		if err == context.Canceled || err == context.DeadlineExceeded {
			return false
		}
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		if res.Code != pb.Code_OK {
			err = wire_errors.FromPBCode(res.Code, res.CodeDes)
			//if remote is not a leader, retry
			if err == wire_errors.NotLeader {
				return true
			}
			return false
		}

		nodeID = res.NodeId
		return false
	}, 500*time.Millisecond)

	return nodeID, err
}


//FIXME: stream layer need Code to tell logic error or network error
func (client *SMClient) CreateStream(ctx context.Context, dataShard uint32, parityShard uint32) (*pb.StreamInfo, *pb.ExtentInfo, error) {	
	err := errors.New("can not find connection to stream manager")
	var res *pb.CreateStreamResponse
	var ei *pb.ExtentInfo
	var si *pb.StreamInfo
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.CreateStream(ctx, &pb.CreateStreamRequest{
			DataShard: dataShard,
			ParityShard: parityShard,
		})

		//user cancel or timeout
		if err == context.Canceled || err == context.DeadlineExceeded {
			return false
		}
		//grpc error
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true //retry
		}
		//logical error
		if res.Code != pb.Code_OK {
			err = wire_errors.FromPBCode(res.Code, res.CodeDes)
			return false
		}
		ei = res.Extent
		si = res.Stream
		err = nil
		return false
	}, 500*time.Millisecond)

	return si, ei, err
}

func (client *SMClient) StreamAllocExtent(ctx context.Context, streamID uint64, 
	extentToSeal uint64, dataShard, parityShard uint32, ownerKey string, revision int64, checkCommitLength uint32) (*pb.ExtentInfo, error) {

	err := errors.New("can not find connection to stream manager")
	var res *pb.StreamAllocExtentResponse
	var ei *pb.ExtentInfo
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.StreamAllocExtent(ctx, &pb.StreamAllocExtentRequest{
			StreamID: streamID,
			ExtentToSeal: extentToSeal,
			DataShard: dataShard,
			ParityShard: parityShard,
			OwnerKey: ownerKey,
			Revision: revision,
			CheckCommitLength: checkCommitLength,
		})
		if err == context.Canceled || err == context.DeadlineExceeded {
			return false
		}
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		if res.Code != pb.Code_OK {
			err = wire_errors.FromPBCode(res.Code, res.CodeDes)
			//if remote is not a leader, retry
			if err == wire_errors.NotLeader {
				return true
			}
			return false
		}

		ei = res.Extent
		return false
	}, 100*time.Millisecond)

	return ei, err	
}


func (client *SMClient) NodesInfo(ctx context.Context) (map[uint64]*pb.NodeInfo, error) {
	
	err := errors.New("can not find connection to stream manager")
	var res *pb.NodesInfoResponse
	var nodeInfos map[uint64]*pb.NodeInfo
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.NodesInfo(ctx, &pb.NodesInfoRequest{})
		if err == context.Canceled || err == context.DeadlineExceeded {
			return false
		}
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		if res.Code != pb.Code_OK {
			err = wire_errors.FromPBCode(res.Code, res.CodeDes)
			//if remote is not a leader, retry
			if err == wire_errors.NotLeader {
				return true
			}
			return false
		}
		nodeInfos = res.Nodes
		return false
	}, 500*time.Millisecond)

	return nodeInfos, err
}


func (client *SMClient) ExtentInfo(ctx context.Context, extentIDs []uint64) (map[uint64]*pb.ExtentInfo, error) {
	
	err := errors.New("can not find connection to stream manager")
	var res *pb.ExtentInfoResponse
	var extentInfos map[uint64]*pb.ExtentInfo
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.ExtentInfo(ctx, &pb.ExtentInfoRequest{Extents: extentIDs})
		if err == context.Canceled || err == context.DeadlineExceeded {
			return false
		}
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		if res.Code != pb.Code_OK {
			err = wire_errors.FromPBCode(res.Code, res.CodeDes)
			//if remote is not a leader, retry
			if err == wire_errors.NotLeader {
				return true
			}
			return false
		}
		extentInfos = res.Extents
		return false
	}, 500*time.Millisecond)

	return extentInfos, err
}




func (client *SMClient) StreamInfo(ctx context.Context, streamIDs []uint64) (map[uint64]*pb.StreamInfo, map[uint64]*pb.ExtentInfo, error) {
	
	err := errors.New("can not find connection to stream manager")
	var res *pb.StreamInfoResponse
	var extentInfos map[uint64]*pb.ExtentInfo
	var streamInfos map[uint64]*pb.StreamInfo

	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.StreamInfo(ctx, &pb.StreamInfoRequest{StreamIDs: streamIDs})
		if err == context.Canceled || err == context.DeadlineExceeded {
			return false
		}
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		if res.Code != pb.Code_OK {
			err = wire_errors.FromPBCode(res.Code, res.CodeDes)
			//if remote is not a leader, retry
			if err == wire_errors.NotLeader {
				return true
			}
			return false
		}
		extentInfos = res.Extents
		streamInfos = res.Streams
		return false
	}, 500*time.Millisecond)

	return streamInfos, extentInfos, err	
}

func (client *SMClient) TruncateStream(ctx context.Context, streamID uint64, extentID uint64) error {
	err := errors.New("can not find connection to stream manager")
	var res *pb.TruncateResponse
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.Truncate(ctx, &pb.TruncateRequest{
			StreamID: streamID,
			ExtentID: extentID,
		})
		if err == context.Canceled || err == context.DeadlineExceeded {
			return false
		}
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		if res.Code != pb.Code_OK {
			err = wire_errors.FromPBCode(res.Code, res.CodeDes)
			//if remote is not a leader, retry
			if err == wire_errors.NotLeader {
				return true
			}
			return false
		}
		return false
	}, 500*time.Millisecond)

	return  err	
}
	

/*
func (client *SMClient) SubmitRecoveryTask(ctx context.Context, extentID uint64, replaceID uint64) error {
	
	err := errors.New("can not find connection to stream manager")
	var res *pb.SubmitRecoveryTaskResponse
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.SubmitRecoveryTask(ctx, &pb.SubmitRecoveryTaskRequest{
			Task : &pb.RecoveryTask{
				ExtentID: extentID,
				ReplaceID: replaceID,
			},
		})
		if err == context.Canceled || err == context.DeadlineExceeded {
			return false
		}
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		if res.Code != pb.Code_OK {
			err = wire_errors.FromPBCode(res.Code, res.CodeDes)
			//if remote is not a leader, retry
			if err == wire_errors.NotLeader {
				return true
			}
			return false
		}

		return false
	}, 500*time.Millisecond)

	return err

}
*/