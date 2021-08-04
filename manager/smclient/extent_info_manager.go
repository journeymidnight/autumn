package smclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"

	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"google.golang.org/grpc"
)


type ExtentManager struct {

	smClient     *SMClient
	extentLock   *utils.SafeMutex
	extentInfo map[uint64]*pb.ExtentInfo

	nodesInfo  map[uint64]*pb.NodeInfo
	nodesLock    *utils.SafeMutex

	client       *clientv3.Client
	closeEtcdResource   func()
	stoper      *utils.Stopper  

	//condition lock to notify WaitVersion
	cond   *sync.Cond
	extentUpdatedCallback extentInfoUpdatedFunc

}


type extentInfoUpdatedFunc func(eventType string, cur *pb.ExtentInfo, prev *pb.ExtentInfo)
type nodesInfoUpdatedFunc func(event *clientv3.Event)



func NewExtentManager(smclient *SMClient, etcdAddr []string, extentsUpdate extentInfoUpdatedFunc) *ExtentManager {

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdAddr,
		DialTimeout: time.Second,
	})

	if err != nil {
		return nil
	}
	/*
	ETCD FORMATE:
	nodes/{id}   => pb.NodeInfo
	extents/{id} => pb.ExtentInfo
	*/
	em := &ExtentManager{
		extentInfo: make(map[uint64]*pb.ExtentInfo),
		nodesInfo: make(map[uint64]*pb.NodeInfo),
		extentLock: &utils.SafeMutex{},
		nodesLock: &utils.SafeMutex{},
		client: client,
		smClient : smclient,
		cond: sync.NewCond(new(sync.Mutex)),
		extentUpdatedCallback: extentsUpdate,
	}
	
	//load latest data
	kvs, _, err := etcd_utils.EtcdRange(client, "nodes/")
	if err != nil {
		xlog.Logger.Error(err)
	} else {
		for _, kv := range kvs {
			var info pb.NodeInfo
			if err = info.Unmarshal(kv.Value) ; err != nil{
				xlog.Logger.Errorf("reflect: can not get pb.NodeInfo")
				continue
			}
			em.nodesInfo[info.NodeID] = &info
		}
	}

	kvs, rev, err := etcd_utils.EtcdRange(client, "extents/")
	if err != nil {
		xlog.Logger.Error(err)
	} else {
		for _, kv := range kvs {
			var info pb.ExtentInfo
			if err = info.Unmarshal(kv.Value) ; err != nil{
				xlog.Logger.Errorf("reflect: can not get pb.extentInfo")
				continue
			}
			em.extentInfo[info.ExtentID] = &info
		}
	}



	//start watch
	stopper := utils.NewStopper()
	nodesChan, close1 := etcd_utils.EtcdWatchEvents(client, "nodes/", "nodes0", rev)
	extentsChan, close2 := etcd_utils.EtcdWatchEvents(client, "extents/", "extents0", rev)

	stopper.RunWorker(func(){
		for {
			select {
			case <- stopper.ShouldStop():
				return
			case res := <- extentsChan:
				for _, e := range res.Events {
					em.extentLock.Lock()
					var info pb.ExtentInfo
					
					switch e.Type.String() {
					case "PUT":
						if err := info.Unmarshal(e.Kv.Value); err != nil {
							xlog.Logger.Errorf("reflect: can not get pb.extentInfo")
							continue
						}
						p := em.getExtentInfo(info.ExtentID)
						if p ==nil || (p !=nil && info.Eversion > p.Eversion) {
							em.extentInfo[info.ExtentID] = &info
						}

						var prevInfo *pb.ExtentInfo
						if e.PrevKv != nil{
							prevInfo = &pb.ExtentInfo{}
							if err := prevInfo.Unmarshal(e.PrevKv.Value); err != nil {
								xlog.Logger.Errorf("reflect: can not get pb.extentInfo")
							}
						}

						if em.extentUpdatedCallback != nil {
							em.extentUpdatedCallback("PUT", &info, prevInfo)
						}
						


					case "DELETE":
						if err := info.Unmarshal(e.PrevKv.Value); err != nil {
							xlog.Logger.Errorf("reflect: can not get pb.NodeInfo")
							continue
						}
						p := em.getExtentInfo(info.ExtentID)
						if p != nil && info.Eversion >= p.Eversion {
							delete(em.extentInfo, info.ExtentID)
						}

						var prevInfo pb.ExtentInfo
						if err := prevInfo.Unmarshal(e.PrevKv.Value); err != nil {
							xlog.Logger.Errorf("reflect: can not get pb.NodeInfo")
							continue
						}

						if em.extentUpdatedCallback != nil {
							em.extentUpdatedCallback("DELETE", nil, &prevInfo)
						}
						
					default:
						panic("")
					}
					em.extentLock.Unlock()
					em.cond.Broadcast()
				}

			case res := <- nodesChan:
				for _, e := range res.Events {
					//update nodes
					em.nodesLock.Lock()

					var info pb.NodeInfo
					switch e.Type.String() {
					case "PUT":
						if err := info.Unmarshal(e.Kv.Value); err != nil {
							xlog.Logger.Errorf("reflect: can not get pb.NodeInfo")
							continue
						}
						em.nodesInfo[info.NodeID] = &info
					case "DELETE":
						if err := info.Unmarshal(e.PrevKv.Value); err != nil {
							xlog.Logger.Errorf("reflect: can not get pb.NodeInfo")
							continue
						}
						delete(em.nodesInfo, info.NodeID)
					default:
						panic("")
					}
					em.nodesLock.Unlock()
				}
			}
		}
	})


	close := func() {
		stopper.Stop()
		close1()
		close2()
	}

	em.closeEtcdResource = close

	return em	
}


func (em *ExtentManager) EtcdClient() *clientv3.Client {
	return em.client
}

func (em *ExtentManager) Close() {
	em.closeEtcdResource()

}
func (em *ExtentManager) GetPeers(extentID uint64) []string {
	extentInfo := em.GetExtentInfo(extentID)

	var ret []string
	for _, id := range extentInfo.Replicates {
		n := em.GetNodeInfo(id)
		utils.AssertTrue(n != nil)
		ret = append(ret, n.Address)
	}
	for _, id := range extentInfo.Parity {
		n := em.GetNodeInfo(id)
		utils.AssertTrue(n != nil)
		ret = append(ret, n.Address)
	}
	return ret
}

type SelectNodePolicy interface {
	Choose(em *ExtentManager, extentID uint64) *grpc.ClientConn
}

type PrimaryPolicy struct{}

func (PrimaryPolicy) Choose(em *ExtentManager, extentID uint64) *grpc.ClientConn {
	exInfo := em.getExtentInfo(extentID)
	if exInfo == nil {
		return nil
	}
	nodeInfo := em.GetNodeInfo(exInfo.Replicates[0])
	pool := conn.GetPools().Connect(nodeInfo.Address)
	if pool == nil || !pool.IsHealthy(){
		return nil
	}
	return pool.Get()
}

type AlivePolicy struct{}

func (AlivePolicy) Choose(em *ExtentManager, extentID uint64) *grpc.ClientConn {	
	exInfo := em.getExtentInfo(extentID)
	if exInfo == nil {
		return nil
	}
	//in EC, if all replicates do not work, so it is meaningless to connect parity nodes
	for i := range exInfo.Replicates {
		if exInfo.Avali > 0 && ((1 << i) & exInfo.Avali) == 0 {
			continue
		}
		nodeInfo := em.GetNodeInfo(exInfo.Replicates[i])

		pool := conn.GetPools().Connect(nodeInfo.Address)
		if pool == nil || !pool.IsHealthy(){
			return nil
		}
		return pool.Get()
	}
	return nil
}

func (em *ExtentManager) GetExtentConn(extentID uint64, policy SelectNodePolicy) *grpc.ClientConn {
	return policy.Choose(em, extentID)
}


/*
//    c.L.Lock()
//    for !condition() {
//        c.Wait()
//    }
//    ... make use of condition ...
//    c.L.Unlock()
*/
func (em *ExtentManager) WaitVersion(extentID uint64, version uint64) *pb.ExtentInfo {
	
	var ei *pb.ExtentInfo
	em.cond.L.Lock()
	for {
		ei = em.GetExtentInfo(extentID)
		if ei != nil && ei.Eversion >= version{
			break
		} else {
			fmt.Printf("extent %d waiting for %d", extentID, version)
			em.cond.Wait()
		}
	}
	em.cond.L.Unlock()
	return ei
}


func (em *ExtentManager) Latest(extentID uint64) *pb.ExtentInfo {
	     
	var ei *pb.ExtentInfo
	for {
		res, err := em.smClient.ExtentInfo(context.Background(), []uint64{extentID})
		if err == nil {
			ei = res[extentID]
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	return ei
}

func (em *ExtentManager) GetNodeInfo(nodeID uint64) *pb.NodeInfo {
	em.nodesLock.RLock()
	defer em.nodesLock.RUnlock()
	info, ok := em.nodesInfo[nodeID]
	if !ok {
		return nil
	}
	return info
}

func (em *ExtentManager) getExtentInfo(extentID uint64) *pb.ExtentInfo {
	info, ok := em.extentInfo[extentID]
	if !ok {
		return nil
	}
	return info
}
func (em *ExtentManager) GetExtentInfo(extentID uint64) *pb.ExtentInfo {
	em.extentLock.RLock()
	defer em.extentLock.RUnlock()
	return em.getExtentInfo(extentID)
}