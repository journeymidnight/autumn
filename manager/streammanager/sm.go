package streammanager

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"go.etcd.io/etcd/clientv3/concurrency"
	"google.golang.org/grpc"
)

var (
	idKey             = "AutumnSmIDKey"
	electionKeyPrefix = "AutumnSmLeader"
)

type NodeStatus struct {
	pb.NodeInfo
	usage    float64
	lastEcho time.Time
}

type StreamManager struct {
	//FIXME: version support
	streamLock utils.SafeMutex
	streams    map[uint64]*pb.StreamInfo

	extentsLock utils.SafeMutex
	extents     map[uint64]*pb.ExtentInfo

	nodeLock utils.SafeMutex
	nodes    map[uint64]*NodeStatus

	etcd       *embed.Etcd
	client     *clientv3.Client
	config     *manager.Config
	grcpServer *grpc.Server
	ID         uint64

	allocIdLock sync.Mutex //used in AllocID

	isLeader    int32
	memberValue string
	//leadeKey is to store Election key
	leaderKey string

	policy AllocExtentPolicy
}

func NewStreamManager(etcd *embed.Etcd, client *clientv3.Client, config *manager.Config) *StreamManager {
	sm := &StreamManager{
		etcd:   etcd,
		client: client,
		config: config,
		ID:     uint64(etcd.Server.ID()),
		policy: new(SimplePolicy),
	}

	v := pb.SMMemberValue{
		ID:      sm.ID,
		Name:    etcd.Config().Name,
		GrpcURL: config.GrpcUrl,
	}

	data, err := v.Marshal()
	utils.Check(err)

	sm.memberValue = string(data)

	return sm

}

func (sm *StreamManager) etcdLeader() uint64 {
	return uint64(sm.etcd.Server.Leader())
}

func (sm *StreamManager) AmLeader() bool {
	return atomic.LoadInt32(&sm.isLeader) == 1
}

func (sm *StreamManager) runAsLeader() {
	//load system

	sm.streamLock.Lock()
	defer sm.streamLock.Unlock()
	sm.extentsLock.Lock()
	defer sm.extentsLock.Unlock()
	sm.nodeLock.Lock()
	defer sm.nodeLock.Unlock()

	//load streams
	kvs, err := manager.EtcdRange(sm.client, "streams")
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return
	}
	sm.streams = make(map[uint64]*pb.StreamInfo)

	for _, kv := range kvs {
		streamID, err := parseKey(string(kv.Key), "streams")
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		var streamInfo pb.StreamInfo
		if err = streamInfo.Unmarshal(kv.Value); err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		sm.streams[streamID] = &streamInfo
	}

	//load extents
	kvs, err = manager.EtcdRange(sm.client, "extents")
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return
	}

	sm.extents = make(map[uint64]*pb.ExtentInfo)

	for _, kv := range kvs {
		extentID, err := parseKey(string(kv.Key), "extents")
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		var extentInfo pb.ExtentInfo
		if err = extentInfo.Unmarshal(kv.Value); err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		sm.extents[extentID] = &extentInfo
	}

	sm.nodes = make(map[uint64]*NodeStatus)

	kvs, err = manager.EtcdRange(sm.client, "nodes")
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return
	}
	for _, kv := range kvs {
		nodeID, err := parseKey(string(kv.Key), "nodes")
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		var nodeInfo pb.NodeInfo
		if err = nodeInfo.Unmarshal(kv.Value); err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		sm.nodes[nodeID] = &NodeStatus{
			NodeInfo: nodeInfo,
		}
	}

	atomic.StoreInt32(&sm.isLeader, 1)
}

func (sm *StreamManager) LeaderLoop() {
	for {
		if sm.ID != sm.etcdLeader() {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		s, err := concurrency.NewSession(sm.client, concurrency.WithTTL(15))
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			continue
		}
		//returns a new election on a given key prefix
		e := concurrency.NewElection(s, electionKeyPrefix)
		ctx := context.TODO()

		if err = e.Campaign(ctx, sm.memberValue); err != nil {
			xlog.Logger.Warnf(err.Error())
			continue
		}
		sm.leaderKey = e.Key()
		xlog.Logger.Infof("elected %d as leader", sm.ID)
		sm.runAsLeader()

		select {
		case <-s.Done():
			s.Close()
			atomic.StoreInt32(&sm.isLeader, 0)
			xlog.Logger.Info("%d's leadershipt expire", sm.ID)
		}
	}
}

/*
func (sm *StreamManager) allocUniqID(count uint64) (uint64, uint64) {
	n := 10
	for {
		start, end, err := sm._allocUniqID(count)
		if err == nil {
			return start, end
		}
		time.Sleep(time.Duration(n) * time.Millisecond)
		n *= 2
	}
}
*/

func (sm *StreamManager) allocUniqID(count uint64) (uint64, uint64, error) {

	var err error
	sm.allocIdLock.Lock()
	defer sm.allocIdLock.Unlock()

	curValue, err := manager.EtcdGetKV(sm.client, idKey)
	if err != nil {
		return 0, 0, err
	}

	//build txn, compare and set ID
	var cmp clientv3.Cmp
	var curr uint64

	if curValue == nil {
		cmp = clientv3.Compare(clientv3.CreateRevision(idKey), "=", 0)
	} else {
		curr = binary.BigEndian.Uint64(curValue)
		cmp = clientv3.Compare(clientv3.Value(idKey), "=", string(curValue))
	}

	var newValue [8]byte
	binary.BigEndian.PutUint64(newValue[:], curr+count)

	txn := clientv3.NewKV(sm.client).Txn(context.Background())
	t := txn.If(cmp)
	_, err = t.Then(clientv3.OpPut(idKey, string(newValue[:]))).Commit()
	if err != nil {
		return 0, 0, err
	}
	return curr, curr + count, nil

}

func (sm *StreamManager) ServeGRPC() error {
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(8<<20),
		grpc.MaxSendMsgSize(8<<20),
		grpc.MaxConcurrentStreams(1000),
	)

	//pb.Reg

	listener, err := net.Listen("tcp", sm.config.GrpcUrl)
	if err != nil {
		return err
	}
	go func() {
		grpcServer.Serve(listener)
	}()
	sm.grcpServer = grpcServer
	return nil
}

func (sm *StreamManager) Close() {

}
