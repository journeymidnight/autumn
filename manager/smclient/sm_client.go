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

func (client *SMClient) Status() error {
	if len(client.conns) == 0 {
		return errors.New("not connection to stream server")
	}
	for i := range client.conns{
		c := pb.NewStreamManagerServiceClient(client.conns[i])
		_, err := c.Status(context.Background(), &pb.StatusRequest{})
		if err != nil {
			return err
		}
	}
	return nil
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


func (client *SMClient) CheckCommitLength(ctx context.Context, streamID uint64, ownerKey string, revision int64) (*pb.StreamInfo , *pb.ExtentInfo, uint32, error){
	err := ErrTimeOut
	var res *pb.CheckCommitLengthResponse
	var stream *pb.StreamInfo
	var lastEx *pb.ExtentInfo
	var end uint32
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.CheckCommitLength(ctx, &pb.CheckCommitLengthRequest{
			StreamID: streamID,
			OwnerKey: ownerKey,
			Revision: revision,
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
		stream = res.StreamInfo
		lastEx = res.LastExInfo
		end = res.End
		return false
	}, 500*time.Millisecond)

	return stream, lastEx, end, err
}

func (client *SMClient) StreamAllocExtent(ctx context.Context, streamID uint64,
	ownerKey string, revision int64, end uint32) (*pb.StreamInfo , *pb.ExtentInfo, error) {

	err := ErrTimeOut
	var res *pb.StreamAllocExtentResponse
	var lastEx *pb.ExtentInfo
	var stream *pb.StreamInfo
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.StreamAllocExtent(ctx, &pb.StreamAllocExtentRequest{
			StreamID: streamID,
			OwnerKey: ownerKey,
			Revision: revision,
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
		stream = res.StreamInfo
		lastEx = res.LastExInfo
		return false
	}, 500*time.Millisecond)

	return stream, lastEx, err
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


func (client *SMClient) ExtentInfo(ctx context.Context, extentID uint64) (*pb.ExtentInfo, error) {
	
	err := ErrTimeOut
	var res *pb.ExtentInfoResponse
	var exInfo *pb.ExtentInfo
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.ExtentInfo(ctx, &pb.ExtentInfoRequest{ExtentID: extentID})
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
		exInfo = res.ExInfo
		return false
	}, 500*time.Millisecond)

	return exInfo, err
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

func (client *SMClient) PunchHoles(ctx context.Context, streamID uint64, holes []uint64, ownerKey string, revision int64) (updatedStream *pb.StreamInfo, err error) {
	err = ErrTimeOut
	var res *pb.PunchHolesResponse
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.StreamPunchHoles(ctx, &pb.PunchHolesRequest{
			StreamID: streamID,
			ExtentIDs: holes,
			Revision: revision,
			OwnerKey: ownerKey,
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

		updatedStream = res.Stream
		return false

	}, 500*time.Millisecond)
	
	return
}


func (client *SMClient) TruncateStream(ctx context.Context, streamID uint64, extentID uint64, ownerKey string, revision int64) (updatedStream *pb.StreamInfo , err error) {
	err = ErrTimeOut
	var res *pb.TruncateResponse
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.Truncate(ctx, &pb.TruncateRequest{
			StreamID: streamID,
			ExtentID: extentID,
			OwnerKey: ownerKey,
			Revision: revision,
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
		updatedStream = res.UpdatedStreamInfo
		return false
	}, 500*time.Millisecond)

	return 
}
	

func (client *SMClient) MultiModifySplit(ctx context.Context, partID uint64, midKey []byte, 
	ownerKey string, revision int64, logEnd, rowEnd uint32) error {
	err := ErrTimeOut
	var res *pb.MultiModifySplitResponse
	client.try(func(conn *grpc.ClientConn) bool {
		c := pb.NewStreamManagerServiceClient(conn)
		res, err = c.MultiModifySplit(ctx, &pb.MultiModifySplitRequest{
			PartID: partID,
			MidKey: midKey,
			OwnerKey: ownerKey,
			Revision: revision,
			LogStreamSealedLength: logEnd,
			RowStreamSealedLength: rowEnd,
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