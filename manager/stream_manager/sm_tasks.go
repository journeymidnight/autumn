package stream_manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

//extent is copied from replaceID to newNodeID
func (sm *StreamManager) copyDone(task *pb.RecoveryTaskStatus) {

	if err := sm.lockExtent(task.Task.ExtentID); err != nil {
		xlog.Logger.Warnf("extent %d has been deleted, remove recovery task", task.Task.ExtentID)
		//extent has been deleted, remove Copy task	from etcd
		taskKey := FormatRecoveryTaskName(task.Task.ExtentID)
		cmps := []clientv3.Cmp{clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue)}
		ops := []clientv3.Op{
			clientv3.OpDelete(taskKey),
		}

		if err := etcd_utils.EtcdSetKVS(sm.client, cmps, ops); err != nil {
			xlog.Logger.Warnf("setting etcd failed [%s]", err)
			return
		}
		return
	}
	defer sm.unlockExtent(task.Task.ExtentID)

	exInfo, ok := sm.cloneExtentInfo(task.Task.ExtentID)
	if !ok {
		return
	}
	newNodeID := task.Task.NodeID

	slot := FindNodeIndex(exInfo, task.Task.ReplaceID)
	if slot == -1 {
		xlog.Logger.Errorf("copyDone invalid task %v", task)
		return
	}

	//replace ID
	if slot < len(exInfo.Replicates) {
		exInfo.Replicates[slot] = newNodeID
		exInfo.ReplicateDisks[slot] = task.ReadyDiskID
	} else {
		exInfo.Parity[slot-len(exInfo.Replicates)] = newNodeID
		exInfo.ParityDisk[slot-len(exInfo.Replicates)] = task.ReadyDiskID
	}
	exInfo.Eversion++

	extentKey := formatExtentKey(exInfo.ExtentID)
	data := utils.MustMarshal(exInfo)

	taskKey := FormatRecoveryTaskName(exInfo.ExtentID)
	cmps := []clientv3.Cmp{clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue)}
	ops := []clientv3.Op{
		clientv3.OpDelete(taskKey),
		clientv3.OpPut(extentKey, string(data)),
	}

	if err := etcd_utils.EtcdSetKVS(sm.client, cmps, ops); err != nil {
		xlog.Logger.Warnf("setting etcd failed [%s]", err)
		return
	}

	sm.extents.Set(exInfo.ExtentID, exInfo)
	//remove from taskPool in local memory
	sm.taskPool.Remove(exInfo.ExtentID)
	//return successfull
	xlog.Logger.Infof("extent %d, replaceID %d is restored on node %d", task.Task.ExtentID, task.Task.ReplaceID, newNodeID)
}

//go routine
func (sm *StreamManager) routineUpdateDF() {

	ticker := utils.NewRandomTicker(10*time.Second, 20*time.Second)

	defer func() {
		xlog.Logger.Infof("routineUpdateDF quit")
	}()

	df := func(node *NodeStatus) {
		defer func() {
			if time.Now().Sub(node.LastEcho()) > 4*time.Minute {
				fmt.Printf("node %d set dead", node.NodeInfo.NodeID)
				//node.SetDead()
			}
		}()

		conn := node.GetConn()
		pctx, pCancel := context.WithTimeout(sm.stopper.Ctx(), 5 * time.Second)
		client := pb.NewExtentServiceClient(conn)
		res, err := client.Df(pctx, &pb.DfRequest{
			Tasks: sm.taskPool.GetFromNode(node.NodeID),
		})
		pCancel()
		if err != nil {
			xlog.Logger.Infof("remote server %s has no response", node.Address)
			return
		}

		//fmt.Printf("df result is %+v\n", res)

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
	fmt.Println("routineUpdateDF")
	for {
		select {
		case <-sm.stopper.ShouldStop():
			return
		case <-ticker.C:
			nodes := sm.getAllNodeStatus(false)
			for i := range nodes {
				node := nodes[i]
				go df(node)
			}
		}
	}
}

type RecoveryTask struct {
	task        *pb.RecoveryTask //immutable
	runningNode uint64
	start       int64 //
}

func (rt *RecoveryTask) StartTime() time.Time {
	val := atomic.LoadInt64(&rt.start)
	return time.Unix(val, 0)
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
	return fmt.Sprintf("recoveryTasks/%d", extentID)
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
func (sm *StreamManager) saveRecoveryTask(task *pb.RecoveryTask) error {
	key := FormatRecoveryTaskName(task.ExtentID)

	//set etcd if isLeader && not exist, submit the task
	data := utils.MustMarshal(task)

	cmps := []clientv3.Cmp{clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
		clientv3.Compare(clientv3.CreateRevision(key), "=", 0)}
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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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

	fmt.Printf("Reavali extent %d is good\n", exInfo.ExtentID)

	slot := FindNodeIndex(exInfo, nodeID)
	if slot == -1 {
		xlog.Logger.Error("maybe updated by others..")
		return
	}
	//update exInfo
	exInfo.Eversion++
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
	fmt.Printf("setting avali for extent:%d on node %d is avali\n", exInfo.ExtentID, nodeID)
}

//dispatchRecoveryTask already have extentLock, ask one node to do recovery
func (sm *StreamManager) dispatchRecoveryTask(exInfo *pb.ExtentInfo, replaceID uint64) error {

	//duplicate?
	if sm.taskPool.HasTask(exInfo.ExtentID) {
		t := sm.taskPool.GetFromExtent(exInfo.ExtentID)
		ns := sm.getNodeStatus(t.NodeID)
		pool := conn.GetPools().Connect(ns.Address)
		if pool != nil && pool.IsHealthy() {
			return errors.Errorf("duplicate task")
		}
	}

	//find a remote node
	nodes := sm.getAllNodeStatus(true)
	if len(nodes) == 0 {
		return errors.Errorf("can not find remote node to copy")
	}

	/*
		if exInfo.ExtentID == 17 {
			chosenNode = &NodeStatus{NodeInfo:pb.NodeInfo{
				NodeID: 4,
				Address:"127.0.0.1:4002",
			}}
		} else {
			fmt.Printf("ignore")
			return nil
		}*/

	//simple policy
	var chosenNode *NodeStatus
	for i := range nodes {
		//to be replaced
		if nodes[i].NodeID == replaceID {
			continue
		}

		//already in node
		if FindNodeIndex(exInfo, nodes[i].NodeID) >= 0 {
			continue
		}

		//no disk avali
		noDiskAvali := true
		for _, diskID := range nodes[i].Disks {
			ds := sm.getDiskStatus(diskID)
			if ds.Online {
				noDiskAvali = false
				break
			}
		}
		if noDiskAvali {
			continue
		}

		chosenNode = nodes[i]
		break
	}

	if chosenNode == nil {
		return errors.New("can not find remote node to copy")
	}

	pool := conn.GetPools().Connect(chosenNode.Address)
	if pool == nil || !pool.IsHealthy() {
		xlog.Logger.Warnf("can not connect %v", chosenNode)
		return errors.Errorf("can not connect %v", chosenNode)
	}

	task := &pb.RecoveryTask{
		ExtentID:  exInfo.ExtentID,   //extentID to be recovered
		ReplaceID: replaceID,         //nodeID in replicas or parity fields, origin location
		NodeID:    chosenNode.NodeID, //nodeID of newlocation
	}
	fmt.Printf("dispatch task %+v to node %+v\n", task, chosenNode)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	c := pb.NewExtentServiceClient(pool.Get())

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
		err = wire_errors.FromPBCode(res.Code, res.CodeDes)
		xlog.Logger.Warnf(err.Error())
		return err
	}

	task.StartTime = time.Now().Unix()
	//remote chosenNode has accept the request
	if err = sm.saveRecoveryTask(task); err != nil {
		xlog.Logger.Warnf(err.Error())
		return err
	}
	sm.taskPool.Insert(task)

	return nil
}

func (sm *StreamManager) routineDispatchTask() {
	//loop over task pool, assign a task to a living node
	ticker := utils.NewRandomTicker(5*time.Second, 10*time.Second)
	defer func() {
		ticker.Stop()
		xlog.Logger.Infof("routineDispatchTask quit")
	}()

	xlog.Logger.Infof("routineDispatchTask started")
	for {
		select {
		case <-sm.stopper.ShouldStop():
			return
		case <-ticker.C:
		OUTER:
			for kv := range sm.extents.Iter() {
				exInfo := kv.Value.(*pb.ExtentInfo) //extent is read only

				//only check sealed extent
				if exInfo.Avali == 0 {
					continue
				}

				if err := sm.lockExtent(exInfo.ExtentID); err != nil {
					xlog.Logger.Warnf("lock extent failed [%s]", err)
					continue
				}

				allCopies := append(exInfo.Replicates, exInfo.Parity...)

				for i, nodeID := range allCopies {

					//check node is avali
					if (exInfo.Avali & uint32(1<<i)) == 0 {
						fmt.Printf("check Avali for extent %d on node %d, avali is %d\n",
							exInfo.ExtentID, nodeID, exInfo.Avali)
						go func(extent *pb.ExtentInfo, nodeID uint64) {
							sm.reAvali(extent, nodeID)
							sm.unlockExtent(extent.ExtentID)
						}(exInfo, nodeID)
						continue OUTER
					}

					//check disk health
					var diskID uint64
					if i >= len(exInfo.Replicates) {
						diskID = exInfo.ParityDisk[i-len(exInfo.Replicates)]
					} else {
						diskID = exInfo.ReplicateDisks[i]
					}

					diskInfo, ok := sm.cloneDiskInfo(diskID)
					if !ok || diskInfo.Online == false {
						go func(exInfo *pb.ExtentInfo, nodeID uint64) {
							err := sm.dispatchRecoveryTask(exInfo, nodeID)
							fmt.Printf("extent %d on disk %d is offline, dispatch result is %v\n", exInfo.ExtentID, diskInfo.DiskID, err)
							sm.unlockExtent(exInfo.ExtentID)
						}(exInfo, nodeID)
						continue OUTER
					}

					//check node health
					ns := sm.getNodeStatus(nodeID)
					if (ns == nil || ns.Dead()) && exInfo.Avali > 0 {
						fmt.Printf("node %d is dead, recovery for %d\n", nodeID, exInfo.ExtentID)
						go func(extent *pb.ExtentInfo, nodeID uint64) {
							if err := sm.dispatchRecoveryTask(extent, nodeID); err != nil {
								xlog.Logger.Warnf("dispatchRecoveryTask failed [%s]", err)
							}
							sm.unlockExtent(extent.ExtentID)
						}(exInfo, nodeID)
						continue OUTER
					}

				}
				sm.unlockExtent(exInfo.ExtentID)
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
