package stream_manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/cornelk/hashmap"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/etcd_utils"
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
	dead     uint32
}

type DiskStatus struct {
	pb.DiskInfo
	total    uint64
	free     uint64
	//avg lantency
}

func (ds *DiskStatus) Total() uint64 {
	return atomic.LoadUint64(&ds.total)
}

func (ds *DiskStatus) Free() uint64 {
	return atomic.LoadUint64(&ds.free)

}

//FIXME: could atomic.LoadPointer make it concise?
func (ns *NodeStatus) Dead() bool {
	return atomic.LoadUint32(&ns.dead) > 0
}
func (ns *NodeStatus) SetDead(){
	atomic.StoreUint32(&ns.dead, 1)
}
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
	streams   *hashmap.HashMap//id => *pb.StreamInfo, read only


	//extents     map[uint64]*pb.ExtentInfo
	//locks is used only for sealed extents when updating avali field and replicates/parities
	extents   *hashmap.HashMap//id => *pb.ExtentInfo, read only
	extentsLocks *sync.Map
	
	
	//nodeLock utils.SafeMutex
	//nodes    map[uint64]*NodeStatus
	nodes     *hashmap.HashMap //id => *NodeStatus
	disks     *hashmap.HashMap  //id => *DiskStatus

	etcd       *embed.Etcd
	client     *clientv3.Client
	config     *manager.Config
	grcpServer *grpc.Server
	ID         uint64 //backend ETCD server's ID

	allocIdLock utils.SafeMutex //used in AllocID

	isLeader    int32
	memberValue string
	//leadeKey is to store Election key
	leaderKey string

	policy AllocExtentPolicy
	stopper *utils.Stopper //leader tasks

	taskPoolLock  *utils.SafeMutex
	taskPool      *TaskPool
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

	startLoading := time.Now()

	//load streams
	kvs, _, err := etcd_utils.EtcdRange(sm.client, "streams")
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

		//FIXME:
		if kv.Version != int64(streamInfo.Sversion) {
			panic(fmt.Sprintf("%s 's Version is not equal to %d", kv.Key, kv.Version, streamInfo.Sversion))
		}

		/*
		Fix if kv.Version != streamInfo.Sversion
		if kv.Version != int64(streamInfo.Sversion) {
			streamInfo.Sversion = uint64(kv.Version)
			etcd_utils.EtcdSetKV(sm.client, formatStreamKey(streamID), utils.MustMarshal(&streamInfo))
		}
		*/
		
		
		sm.streams.Set(streamID, &streamInfo)
	}

	//load extents
	kvs, _, err = etcd_utils.EtcdRange(sm.client, "extents")
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return
	}

	sm.extents = &hashmap.HashMap{}
	sm.extentsLocks = new(sync.Map)
	for _, kv := range kvs {
		extentID, err := parseKey(string(kv.Key), "extents")
		if err != nil {
			xlog.Logger.Errorf(err.Error())
			return
		}
		var extentInfo pb.ExtentInfo
		if err = extentInfo.Unmarshal(kv.Value); err != nil {
			xlog.Logger.Errorf(err.Error())
			return
		}
		
		
		if kv.Version != int64(extentInfo.Eversion) {
			panic(fmt.Sprintf("%s 's Version %d is not equal to %d", kv.Key, kv.Version, extentInfo.Eversion))
		}
		
		sm.extents.Set(extentID, &extentInfo)
		sm.extentsLocks.Store(extentID, new(sync.Mutex))
	}

	//sm.nodes = make(map[uint64]*NodeStatus)

	sm.nodes = &hashmap.HashMap{}

	kvs, _, err = etcd_utils.EtcdRange(sm.client, "nodes")
	if err != nil {
		xlog.Logger.Errorf(err.Error())
		return
	}
	for _, kv := range kvs {
		nodeID, err := parseKey(string(kv.Key), "nodes")
		if err != nil {
			xlog.Logger.Errorf(err.Error())
			return
		}
		var nodeInfo pb.NodeInfo
		if err = nodeInfo.Unmarshal(kv.Value); err != nil {
			xlog.Logger.Errorf(err.Error())
			return
		}
		sm.nodes.Set(nodeID, &NodeStatus{
			NodeInfo: nodeInfo,
		})
	}

	sm.taskPool = NewTaskPool()
	kvs, _, err = etcd_utils.EtcdRange(sm.client, "recoveryTasks")
	if err != nil {
		xlog.Logger.Errorf(err.Error())
		return
	}
	for _, kv := range kvs {
		extentID, err := parseKey(string(kv.Key), "recoveryTasks")
		if err != nil {
			xlog.Logger.Errorf(err.Error())
			return
		}

		var task pb.RecoveryTask
		if err = task.Unmarshal(kv.Value); err != nil {
			xlog.Logger.Errorf(err.Error())
			return
		}

		sm.taskPool.Insert(extentID, &task)
	}

	sm.disks = &hashmap.HashMap{}
	kvs, _, err = etcd_utils.EtcdRange(sm.client, "disks")
	if err != nil {
		xlog.Logger.Errorf(err.Error())
		return
	}
	for _, kv := range kvs {
		diskID, err := parseKey(string(kv.Key), "disks")
		if err != nil {
			xlog.Logger.Errorf(err.Error())
			return
		}
		var diskInfo pb.DiskInfo
		if err = diskInfo.Unmarshal(kv.Value); err != nil {
			xlog.Logger.Errorf(err.Error())
			return
		}
		sm.disks.Set(diskID, &DiskStatus{
			DiskInfo: diskInfo,
		})
	}


	//start leader tasks
	sm.stopper.RunWorker(sm.routineUpdateDF)
	sm.stopper.RunWorker(sm.routineDispatchTask)

	fmt.Printf("Start loading data from etcd, cost %v\n", time.Now().Sub(startLoading))
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

	return etcd_utils.EtcdAllocUniqID(sm.client, idKey, count)
}

func (sm *StreamManager) RegisterGRPC(grpcServer *grpc.Server) {
	pb.RegisterStreamManagerServiceServer(grpcServer, sm)
	sm.grcpServer = grpcServer
}

func (sm *StreamManager) Close() {
	if sm.isLeader > 0 {
		sm.stopper.Stop()
	}
}
