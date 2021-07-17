package stream_manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

//extent is copied from replaceID to newNodeID
func (sm *StreamManager) copyDone(task *pb.RecoveryTask) {

	sm.lockExtent(task.ExtentID)
	defer sm.unlockExtent(task.ExtentID)

	extentInfo, ok := sm.cloneExtentInfo(task.ExtentID)
	if !ok {
		return
	}
	newNodeID := task.NodeID

	slot := FindNodeIndex(extentInfo, task.ReplaceID)
	if slot == -1 {
		xlog.Logger.Errorf("copyDone invalid task %v", task)
		return 
	}

	//replace ID
	if slot < len(extentInfo.Replicates) {
		extentInfo.Replicates[slot] = newNodeID
	} else {
		extentInfo.Parity[slot - len(extentInfo.Replicates)] = newNodeID
	}
	extentInfo.Eversion ++


	extentKey := formatExtentKey(extentInfo.ExtentID)
	data := utils.MustMarshal(extentInfo)

	taskKey := FormatRecoveryTaskName(extentInfo.ExtentID)
	cmps := []clientv3.Cmp{clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue)}
	ops := []clientv3.Op{
		clientv3.OpDelete(taskKey),
		clientv3.OpPut(extentKey, string(data)),
	}

	if err := etcd_utils.EtcdSetKVS(sm.client, cmps, ops); err != nil {
		xlog.Logger.Warnf("setting etcd failed [%s]", err)
		return
	}

	//remove from taskPool in local memory
	sm.taskPool.Remove(extentInfo.ExtentID)
	//return successfull
	xlog.Logger.Infof("extent %d, replaceID %d is restored on node %d", task.ExtentID, task.ReplaceID, newNodeID)
}


//go routine
func (sm *StreamManager) routineUpdateDF() {

	ticker := utils.NewRandomTicker(10 * time.Second, 20 * time.Second)

	var ctx context.Context
	var cancel context.CancelFunc

	defer func() {
		xlog.Logger.Infof("routineUpdateDF quit")
	}()


	df := func(node *NodeStatus){
		defer func(){
			if time.Now().Sub(node.LastEcho()) > 4 * time.Minute {
				fmt.Printf("node %d set dead", node.NodeInfo.NodeID)
				//node.SetDead()
			}
		}()
		
		conn := node.GetConn()
		pctx, pCancel := context.WithTimeout(ctx, 5 * time.Second)
		client := pb.NewExtentServiceClient(conn)
		res, err := client.Df(pctx, &pb.DfRequest{
			Tasks: sm.taskPool.GetFromNode(node.NodeID),
		})
		pCancel()
		if err != nil {
			xlog.Logger.Infof("remote server %s has no response", node.Address)
			return
		}
		if res.Code != pb.Code_OK {
			xlog.Logger.Infof("remote server has error %s", res.CodeDes)
			return
		}

		sumFree := uint64(0)
		sumTotal := uint64(0)
		for diskID, df := range res.DiskStatus {
			disk := sm.getDiskStatus(diskID)
			atomic.StoreUint64(&disk.total, df.Total)
			atomic.StoreUint64(&disk.free, df.Free)
		}

		node.SetFree(sumFree)
		node.SetTotal(sumTotal)
		
		for i := range res.DoneTask {
			sm.copyDone(res.DoneTask[i])
		}
	}



	xlog.Logger.Infof("routineUpdateDF started")
	for {
		select {
			case <- sm.stopper.ShouldStop():
				if cancel != nil{
					cancel()
				}
				return
			case <- ticker.C:
				ctx, cancel = context.WithCancel(context.Background())
				nodes := sm.getAllNodeStatus(false)
				for i := range nodes {
					node := nodes[i]
					go df(node)
				}	
		}
	}
}

type RecoveryTask struct {
	task *pb.RecoveryTask //immutable
	runningNode uint64
	start int64 //
}


func (rt *RecoveryTask) StartTime() time.Time {
	val := atomic.LoadInt64(&rt.start)
	return time.Unix(val,0)
}

func (rt *RecoveryTask) SetStartTime() {
	atomic.StoreInt64(&rt.start, time.Now().Unix())
}

func (rt *RecoveryTask) RunningNode() uint64 {
	return atomic.LoadUint64(&rt.runningNode)
}

func (rt *RecoveryTask) SetRunningNode(x uint64) {
	atomic.StoreUint64(&rt.runningNode, x)
}


func FormatRecoveryTaskName(extentID uint64) string {
	return fmt.Sprintf("recoveryTasks/%d.tsk", extentID)
}

func isReplaceIDinInfo(extentInfo *pb.ExtentInfo, replaceID uint64) bool {

	for i := range extentInfo.Replicates {
		if extentInfo.Replicates[i] == replaceID {
			return true
		}
	}
	for i := range extentInfo.Parity {
		if extentInfo.Parity[i] == replaceID {
			return true
		}
	}
	return false
}

//non-block
func (sm *StreamManager) saveRecoveryTask(task *pb.RecoveryTask) error{
		key := FormatRecoveryTaskName(task.ExtentID)

		//set etcd if isLeader && not exist, submit the task
		data := utils.MustMarshal(task)

		cmps := []clientv3.Cmp{clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
							  clientv3.Compare(clientv3.Version(key), "=", 0)}
		ops := []clientv3.Op{
			clientv3.OpPut(key, string(data)),
		}
		err := etcd_utils.EtcdSetKVS(sm.client, cmps, ops)
		if err != nil {
			xlog.Logger.Warnf("can not submit recoverytask for %s", key)
			return err
		}

		
	return nil
}

//FIXME: delete Extent

//lockExtent returns error to indicate extent has been deleted
func (sm *StreamManager) lockExtent(extentID uint64) error {
	v, ok := sm.extentsLocks.Load(extentID)
	if !ok {
		return errors.New("extent has been deleted")
	}
	v.(*sync.Mutex).Lock()
	return nil
}

func (sm *StreamManager) unlockExtent(extentID uint64) {
	v, ok := sm.extentsLocks.Load(extentID)
	if ok {
		v.(*sync.Mutex).Unlock()
	}
}


func (sm *StreamManager) reAvali(exInfo *pb.ExtentInfo, nodeID uint64) {

	ns := sm.getNodeStatus(nodeID)
	pool := conn.GetPools().Connect(ns.Address)
	if pool == nil {
		return 
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
	c := pb.NewExtentServiceClient(pool.Get())
	res, err := c.ReAvali(ctx, &pb.ReAvaliRequest{
		ExtentID: exInfo.ExtentID,
		Eversion: exInfo.Eversion,
	})
	cancel()
	if err != nil {
		xlog.Logger.Error(err.Error())
		return 
	}
	if res.Code != pb.Code_OK {
		xlog.Logger.Error(res.CodeDes)
		return
	}

	slot := FindNodeIndex(exInfo, nodeID)
	if slot == -1 {
		xlog.Logger.Error("maybe updated by others..")
		return
	}
	//update exInfo
	exInfo.Eversion ++
	exInfo.Avali |= (1 << slot)

	//update etcd
	extentKey := formatExtentKey(exInfo.ExtentID)
	data := utils.MustMarshal(exInfo)

	cmps := []clientv3.Cmp{clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue)}
	ops := []clientv3.Op{
		clientv3.OpPut(extentKey, string(data)),
	}
	err = etcd_utils.EtcdSetKVS(sm.client, cmps, ops)
	if err != nil {
		xlog.Logger.Warnf("setting etcd failed [%s]", err)
		return
	}

	//update extent
	sm.extents.Set(exInfo.ExtentID, exInfo)
}

//dispatchRecoveryTask already have extentLock, ask one node to do recovery
func (sm *StreamManager) dispatchRecoveryTask(exInfo *pb.ExtentInfo, replaceID uint64) error {

	//duplicate?
	if sm.taskPool.HasTask(exInfo.ExtentID) {
		t := sm.taskPool.GetFromExtent(exInfo.ExtentID)
		ns := sm.getNodeStatus(t.NodeID)
		pool := conn.GetPools().Connect(ns.Address)
		if pool != nil &&  pool.IsHealthy() {
			return errors.Errorf("duplicate task")
		}
	}

	//find a remote node
	nodes := sm.getAllNodeStatus(true)
	if len(nodes) == 0 {
		return errors.Errorf("can not find remote node to copy")
	}

	var chosenNode *NodeStatus
	for i := range nodes {
		if nodes[i].NodeID == replaceID {
			continue
		}
		if FindNodeIndex(exInfo, nodes[i].NodeID) >= 0 {
			continue
		}
		chosenNode = nodes[i]
	}


	pool := conn.GetPools().Connect(chosenNode.Address)
	if pool == nil || pool.IsHealthy() == false {
		xlog.Logger.Warnf("can not connect %v", chosenNode)
		return errors.Errorf("can not connect %v", chosenNode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	c := pb.NewExtentServiceClient(pool.Get())
	task := &pb.RecoveryTask{
		ExtentID: exInfo.ExtentID,
		ReplaceID: replaceID,
		NodeID: chosenNode.NodeID,
	}
	res, err := c.RequireRecovery(ctx, &pb.RequireRecoveryRequest{
			Task: task})
	cancel()
	//network error
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return err
	}
	//logic error
	if res.Code != pb.Code_OK {
		xlog.Logger.Warnf(res.CodeDes)
		return err
	}

	task.StartTime = time.Now().Unix()
	//remote chosenNode has accept the request
	if err = sm.saveRecoveryTask(task); err != nil {
		xlog.Logger.Warnf(err.Error())
		return err
	}
	sm.taskPool.Insert(exInfo.ExtentID, task)
	
	return nil
}

func (sm *StreamManager) routineDispatchTask() {
	//loop over task pool, assign a task to a living node
	ticker := utils.NewRandomTicker(5* time.Second, 10 * time.Second)
	defer func() {
		ticker.Stop()
		xlog.Logger.Infof("routineDispatchTask quit")
	}()

	xlog.Logger.Infof("routineDispatchTask started")

	for {
		select {
			case <- sm.stopper.ShouldStop():
				return
			case <- ticker.C:
				OUTER:
				for kv := range sm.extents.Iter() {
					extent := kv.Value.(*pb.ExtentInfo) //extent is read only

					//only check sealed extent
					if extent.SealedLength == 0 {
						continue 
					}
					
					sm.lockExtent(extent.ExtentID)

					allCopies := append(extent.Replicates, extent.Parity...)
					
					for i, nodeID := range allCopies {

						//check disk health
						var diskID uint64
						if i >= len(extent.Replicates) {
							diskID = extent.ParityDisk[i - len(extent.Replicates)]
						} else {
							diskID = extent.ReplicateDisks[i]
						}
						
						diskInfo, ok := sm.cloneDiskInfo(diskID)
						if !ok || diskInfo.Online == 0 {
							go func(extent *pb.ExtentInfo, nodeID uint64) {
								sm.dispatchRecoveryTask(extent, nodeID)
								sm.unlockExtent(extent.ExtentID)
							}(extent, nodeID)
							continue OUTER
						}


						//check node health
						ns := sm.getNodeStatus(nodeID)
						if (ns == nil || ns.Dead()) && extent.SealedLength > 0{
							fmt.Printf("node %d is dead, recovery for %d\n", nodeID, extent.ExtentID) 
							go func(extent *pb.ExtentInfo, nodeID uint64) {
								sm.dispatchRecoveryTask(extent, nodeID)
								sm.unlockExtent(extent.ExtentID)
							}(extent, nodeID)
							continue OUTER
						}

						//check node is avali
						if extent.Avali & uint32(1 << i) == 0 {
							go func(extent *pb.ExtentInfo, nodeID uint64){
								sm.reAvali(extent, nodeID)
								sm.unlockExtent(extent.ExtentID)
							}(extent, nodeID)
							continue OUTER
						}

					}
					sm.unlockExtent(extent.ExtentID)
				}
			}
	}
}


func FindNodeIndex(extentInfo *pb.ExtentInfo, nodeID uint64) int {
	slot := -1
	for i := range extentInfo.Replicates {
		if extentInfo.Replicates[i] == nodeID {
			slot = i
		}
	}

	for j := range extentInfo.Parity {
		if extentInfo.Parity[j] == nodeID {
			if slot == -1 {
				slot = j + len(extentInfo.Replicates)
			} else {
				xlog.Logger.Warnf("replaceID is %d, extentInfo is %v, can not find replaceID\n", nodeID, extentInfo)
				return -1
			}
		}
	}

	return slot 
}