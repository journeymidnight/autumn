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
	ErrTimeOut   = errors.New("can not find connection to stream manager, timeout")
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

func (client *SMClient) RegisterNode(ctx context.Context, uuids []string, addr string) (uint64, map[string]uint64, error) {
	err := ErrTimeOut
	var res *pb.RegisterNodeResponse
	nodeID := uint64(0)
	var uuidToDiskID map[string]uint64
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.RegisterNode(ctx, &pb.RegisterNodeRequest{
			Addr: addr,
			DiskUUIDs: uuids,
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
		uuidToDiskID = res.DiskUUIDs
		return false
	}, 500*time.Millisecond)

	return nodeID, uuidToDiskID,  err
}


//FIXME: stream layer need Code to tell logic error or network error
func (client *SMClient) CreateStream(ctx context.Context, dataShard uint32, parityShard uint32) (*pb.StreamInfo, *pb.ExtentInfo, error) {	
	err := ErrTimeOut
	
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


/*
end表明在stream client层, 明确确认的已经写完成的end(end在写入成功后自动更新), 在Seal时, 如果存在end, 
, sm只需要seal而不需要读各个extent长度,节省运行时间
checkCommitLength:1 , end:0,  readEntries for logStream
checkCommitLength:0,  end:0,  append tables on rowstream, and meet an error
checkCommitLength:0,  end:N,  append log/row stream, meet an error or do rotate   
*/

func (client *SMClient) StreamAllocExtent(ctx context.Context, streamID uint64, Sversion int64,
	extentToSeal uint64, ownerKey string, revision int64, checkCommitLength uint32, end uint32) (*pb.ExtentInfo, error) {

	err := ErrTimeOut
	var res *pb.StreamAllocExtentResponse
	var ei *pb.ExtentInfo
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.StreamAllocExtent(ctx, &pb.StreamAllocExtentRequest{
			StreamID: streamID,
			ExtentToSeal: extentToSeal,
			OwnerKey: ownerKey,
			Revision: revision,
			CheckCommitLength: checkCommitLength,
			Sversion: Sversion,
			End: end,
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
	
	err := ErrTimeOut
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
	
	err := ErrTimeOut
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
	
	err := ErrTimeOut
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

func (client *SMClient) TruncateStream(ctx context.Context, streamID uint64, extentID uint64, ownerKey string, revision int64, sversion int64, gabageKey string) (*pb.GabageStreams ,error) {
	err := ErrTimeOut
	var res *pb.TruncateResponse
	var gabageStreams *pb.GabageStreams
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.Truncate(ctx, &pb.TruncateRequest{
			StreamID: streamID,
			ExtentID: extentID,
			OwnerKey: ownerKey,
			Revision: revision,
			Sversion: sversion,
			GabageKey: gabageKey,
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
		gabageStreams = res.Gabages
		return false
	}, 500*time.Millisecond)

	return gabageStreams, err	
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