package stream_manager

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

//extent is copied from replaceID to newNodeID
func (sm *StreamManager) copyDone(task *pb.RecoveryTask, newNodeID uint64) {
	extentInfo, ok := sm.cloneExtentInfo(task.ExtentID)
	if !ok {
		return
	}

	slot := FindReplaceSlot(extentInfo, task.ReplaceID)
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
//or use gossip protocol in the future
func (sm *StreamManager) routineUpdateDF() {

	ticker := utils.NewRandomTicker(time.Minute, 2 * time.Minute)

	var ctx context.Context
	var cancel context.CancelFunc

	defer func() {
		xlog.Logger.Infof("routineUpdateDF quit")
	}()

	df := func(node *NodeStatus){
		defer func(){
			if time.Now().Sub(node.LastEcho()) > 20 * time.Minute {
				node.SetDead()
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
			xlog.Logger.Infof("remote server %s not response or to", node.Address)
			return
		}
		if res.Code != pb.Code_OK {
			xlog.Logger.Infof("remote server has error %s", res.CodeDes)
			return
		}
		node.SetFree(res.Df.Free)
		node.SetTotal(res.Df.Total)
		for i := range res.DoneTask {
			sm.copyDone(res.DoneTask[i], node.NodeID)
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
				nodes := sm.getAllNodeStatus(true)
				for i := range nodes {
					node := nodes[i]
					df(node)
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


//non-block
//FIXME: if task is running, do not submit it again
func (sm *StreamManager) dispatchRecoveryTask(extentID, replaceID uint64) error {

	//duplicate?
	if sm.taskPool.HasTask(extentID) {
		t := sm.taskPool.GetFromExtent(extentID)
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
		ExtentID: extentID,
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
	sm.taskPool.Insert(extentID, task)
	
	return nil
}

func (sm *StreamManager) routineDispatchTask() {
	//loop over task pool, assign a task to a living node
	ticker := utils.NewRandomTicker(time.Minute, 2 * time.Minute)
	defer func() {
		xlog.Logger.Infof("routineDispatchTask quit")
	}()

	xlog.Logger.Infof("routineDispatchTask started")


	for {
		select {
			case <- sm.stopper.ShouldStop():
				return
			case <- ticker.C:
				for kv := range sm.extents.Iter() {
					extent := kv.Value.(*pb.ExtentInfo) //extent is read only
					for _, nodeID := range extent.Replicates {
						ns := sm.getNodeStatus(nodeID)
						if (ns == nil || ns.Dead()) && extent.SealedLength > 0{
							go func() {
								sm.dispatchRecoveryTask(extent.ExtentID, nodeID)
							}()
						}
					}

					for _, nodeID := range extent.Parity {
						ns := sm.getNodeStatus(nodeID)
						if (ns == nil || ns.Dead()) && extent.SealedLength > 0 {
							go func() {
								sm.dispatchRecoveryTask(extent.ExtentID, nodeID)
							}()
						}
					}
					
				}
			}
	}
}

//node service,
//producer
func (sm *StreamManager) SubmitRecoveryTask(ctx context.Context, req *pb.SubmitRecoveryTaskRequest) (*pb.SubmitRecoveryTaskResponse, error) {
	errDone := func(err error) (*pb.SubmitRecoveryTaskResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.SubmitRecoveryTaskResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

	if !sm.AmLeader() {
		return errDone(wire_errors.NotLeader)
	}

	if err := sm.dispatchRecoveryTask(req.Task.ExtentID, req.Task.ReplaceID); err != nil {
		return errDone(err)
	}

	return &pb.SubmitRecoveryTaskResponse{
		Code: pb.Code_OK,
	}, nil
}


func FindReplaceSlot(extentInfo *pb.ExtentInfo, replaceID uint64) int {
	slot := -1
	for i := range extentInfo.Replicates {
		if extentInfo.Replicates[i] == replaceID {
			slot = i
		}
	}

	for j := range extentInfo.Parity {
		if extentInfo.Parity[j] == replaceID {
			if slot == -1 {
				slot = j + len(extentInfo.Replicates)
			} else {
				xlog.Logger.Warnf("replaceID is %d, extentInfo is %v, can not find replaceID\n", replaceID, extentInfo)
				return -1
			}
		}
	}

	return slot 
}