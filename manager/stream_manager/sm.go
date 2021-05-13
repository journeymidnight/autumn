package stream_manager

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/cornelk/hashmap"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"go.etcd.io/etcd/clientv3/concurrency"
	"google.golang.org/grpc"
)

const (
	idKey             = "AutumnSMIDKey"
	electionKeyPrefix = "AutumnSMLeader"
)

type NodeStatus struct {
	pb.NodeInfo
	//atomic 
	total    uint64
	free     uint64
}

//FIXME: could atomic.LoadPointer make it concise?
func (ns *NodeStatus) Total() uint64{
	return atomic.LoadUint64(&ns.total)
}
func (ns *NodeStatus) SetTotal(t uint64) {
	atomic.StoreUint64(&ns.total, t)
} 
func (ns *NodeStatus) Free() uint64{
	return atomic.LoadUint64(&ns.free)
}
func (ns *NodeStatus) SetFree(f uint64) {
	atomic.StoreUint64(&ns.free, f)
}

func (ns *NodeStatus) GetConn() *grpc.ClientConn {
	pool := conn.GetPools().Connect(ns.Address)
	if pool == nil {
		return nil
	}
	return pool.Get()
}
func (ns *NodeStatus) LastEcho() time.Time {
	pool := conn.GetPools().Connect(ns.Address)
	if pool == nil {
		return time.Time{}
	}
	return pool.LastEcho()
}

func (ns *NodeStatus) IsHealthy() bool {
	pool := conn.GetPools().Connect(ns.Address)
	if pool == nil {
		return false
	}
	return pool.IsHealthy()
}


type StreamManager struct {
	//streams    map[uint64]*pb.StreamInfo
	streams   *hashmap.HashMap//id => *pb.StreamInfo


	//extents     map[uint64]*pb.ExtentInfo
	extents   *hashmap.HashMap//id => *pb.ExtentInfo

	
	//nodeLock utils.SafeMutex
	//nodes    map[uint64]*NodeStatus
	nodes      *hashmap.HashMap //id => *NodeStatus


	etcd       *embed.Etcd
	client     *clientv3.Client
	config     *manager.Config
	grcpServer *grpc.Server
	ID         uint64 //backend ETCD server's ID

	allocIdLock sync.Mutex //used in AllocID

	isLeader    int32
	memberValue string
	//leadeKey is to store Election key
	leaderKey string

	policy AllocExtentPolicy
	stopper *utils.Stopper //leader tasks
}

func NewStreamManager(etcd *embed.Etcd, client *clientv3.Client, config *manager.Config) *StreamManager {
	sm := &StreamManager{
		etcd:   etcd,
		client: client,
		config: config,
		ID:     uint64(etcd.Server.ID()),
		policy: new(SimplePolicy),
		stopper: utils.NewStopper(),
	}
	

	v := pb.MemberValue{
		ID:      sm.ID,
		Name:    etcd.Config().Name + "_SM",
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

	//sm.streamLock.Lock()
	//defer sm.streamLock.Unlock()
	//sm.extentsLock.Lock()
	//defer sm.extentsLock.Unlock()
	//sm.nodeLock.Lock()
	//defer sm.nodeLock.Unlock()

	//load streams
	kvs, err := manager.EtcdRange(sm.client, "streams")
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return
	}
	//sm.streams = make(map[uint64]*pb.StreamInfo)
	sm.streams = &hashmap.HashMap{}

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
		sm.streams.Set(streamID, &streamInfo)
		//sm.streams[streamID] = &streamInfo
	}

	//load extents
	kvs, err = manager.EtcdRange(sm.client, "extents")
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return
	}

	//sm.extents = make(map[uint64]*pb.ExtentInfo)

	sm.extents = &hashmap.HashMap{}
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
		sm.extents.Set(extentID, &extentInfo)
	}

	//sm.nodes = make(map[uint64]*NodeStatus)

	sm.nodes = &hashmap.HashMap{}

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
		sm.nodes.Set(nodeID, &NodeStatus{
			NodeInfo: nodeInfo,
		})
	}

	//start leader tasks
	sm.stopper.RunWorker(sm.routineUpdateDF)
	sm.stopper.RunWorker(sm.routineFixReplics)
	sm.stopper.RunWorker(sm.routineDispatchTask)

	atomic.StoreInt32(&sm.isLeader, 1)
}




func (sm *StreamManager) LeaderLoop() {
	for {
		if sm.ID != sm.etcdLeader() {
			time.Sleep(100 * time.Millisecond)
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
			sm.stopper.Stop()
			xlog.Logger.Info("%d's leadershipt expire", sm.ID)
		}
	}
}

func (sm *StreamManager) allocUniqID(count uint64) (uint64, uint64, error) {

	sm.allocIdLock.Lock()
	defer sm.allocIdLock.Unlock()

	return manager.EtcdAllocUniqID(sm.client, idKey, count)
}

func (sm *StreamManager) RegisterGRPC(grpcServer *grpc.Server) {
	pb.RegisterStreamManagerServiceServer(grpcServer, sm)
	sm.grcpServer = grpcServer
}

func (sm *StreamManager) Close() {
	sm.stopper.Stop()
}
