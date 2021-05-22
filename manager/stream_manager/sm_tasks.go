package stream_manager

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/dlock"
	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

//go routine
//or use gossip protocol in the future
func (sm *StreamManager) routineUpdateDF() {

	ticker := utils.NewRandomTicker(time.Minute, 2 * time.Minute)

	var ctx context.Context
	var cancel context.CancelFunc
	defer func() {
		xlog.Logger.Infof("routineUpdateDF quit")
	}()

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
				stopper := utils.NewStopper()
				for i := range nodes {
					node := nodes[i]
					stopper.RunWorker(func(){
						defer func() {//if no response for 20 minutes, auto set to dead
							if time.Now().Sub(node.LastEcho()) > 20 * time.Minute {
								node.SetDead()
							}
						}()
						conn := node.GetConn()
						pctx, pCancel := context.WithTimeout(ctx, 5 * time.Second)
						client := pb.NewExtentServiceClient(conn)
						res, err := client.Df(pctx, &pb.DfRequest{})
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
					})

				}
				stopper.Wait()

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

func FormatRecoveryTaskLock(extentID uint64) string {
	return fmt.Sprintf("recoveryTaskLocks/%d.lck", extentID)
}

func FormatRecoveryTaskName(extentID uint64, replaceID uint64) string {
	return fmt.Sprintf("recoveryTasks/%d_%d.tsk", extentID)
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
func (sm *StreamManager) queueRecoveryTask(extentID uint64, replaceNodeID uint64) error{
		key := FormatRecoveryTaskName(extentID, replaceNodeID)

		//if we have record for this task, return
		_, ok := sm.taskPool.Get(key)
		if ok {
			return errors.New("duplicate task")
		}

		
		rt := &pb.RecoveryTask{
			ExtentID: extentID,
			ReplaceID: replaceNodeID,
		}

		//set etcd if isLeader && not exist, submit the task
		data := utils.MustMarshal(rt)

		cmps := []clientv3.Cmp{clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
							  clientv3.Compare(clientv3.Version(key), "=", 0)}
		ops := []clientv3.Op{
			clientv3.OpPut(key, string(data)),
		}
		err := manager.EtcdSetKVS(sm.client, cmps, ops)
		if err != nil {
			xlog.Logger.Warnf("can not submit recoverytask for %s", key)
			return err
		}
		sm.taskPool.Set(key, &RecoveryTask{
			task: rt,
		})
		
	return nil
}


//producer
func (sm *StreamManager) routineFixReplics() {
	//loop over all extent, if extent has dead node.
	//insert task into "task pool"
	ticker := utils.NewRandomTicker(time.Minute, 2 * time.Minute)
	defer func() {
		xlog.Logger.Infof("routineFixReplics quit")
	}()

	xlog.Logger.Infof("routineFixReplics started")

	for {
		select {
			case <- sm.stopper.ShouldStop():
				return
			case <- ticker.C:
				//FULL SEARCH
				for kv := range sm.extents.Iter() {
					extent := kv.Value.(*pb.ExtentInfo) //extent is read only
					for _, nodeID := range extent.Replicates {
						ns := sm.getNodeStatus(nodeID)
						if (ns == nil || ns.Dead()) && extent.SealedLength > 0{
							go func() {
								sm.queueRecoveryTask(extent.ExtentID, nodeID)
							}()
						}
					}

					for _, nodeID := range extent.Parity {
						ns := sm.getNodeStatus(nodeID)
						if (ns == nil || ns.Dead()) && extent.SealedLength > 0 {
							go func() {
								sm.queueRecoveryTask(extent.ExtentID, nodeID)
							}()
						}
					}
					
				}
		}
	}
	
}

//non-block
//FIXME: if task is running, do not submit it again
func (sm *StreamManager) dispatchRecoveryTask(t *RecoveryTask) {
	go func() {
		//find a remote node
		nodes := sm.getAllNodeStatus(true)
		if len(nodes) == 0 {
			return
		}
		var chosenNode *NodeStatus
		for i := range nodes {
			if nodes[i].NodeID == t.task.ExtentID {
				continue
			}
			chosenNode = nodes[i]
		}

		pool := conn.GetPools().Connect(chosenNode.Address)
		if pool == nil || pool.IsHealthy() == false {
			xlog.Logger.Warnf("can not connect %v", chosenNode)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		c := pb.NewExtentServiceClient(pool.Get())
		res, err := c.RequireRecovery(ctx, &pb.RequireRecoveryRequest{
			Task: &pb.RecoveryTask{
				ExtentID: t.task.ExtentID,
				ReplaceID: t.task.ReplaceID,
		}})
		cancel()
		//network error
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		//logic error
		if res.Code != pb.Code_OK {
			xlog.Logger.Warnf(res.CodeDes)
			return
		}
		t.SetRunningNode(chosenNode.NodeID)
		t.SetStartTime()
	}()
}

//consumer
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
				for kv := range sm.taskPool.Iter() {
					t := kv.Value.(*RecoveryTask)
					//FIXME, if no runningNode and startTime is long ago, retry it
					if t.RunningNode() == 0 && time.Now().Sub(t.StartTime()) > 5 * time.Minute {
						sm.dispatchRecoveryTask(t)
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

	err := sm.queueRecoveryTask(req.Task.ExtentID, req.Task.ReplaceID)
	if err != nil {
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

//node service, but it's about recovery task
func (sm *StreamManager) CopyExtentDone(ctx context.Context, req *pb.CopyExtentDoneRequeset) (*pb.CopyExtentDoneResponse, error) {
	
	errDone := func(err error) (*pb.CopyExtentDoneResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.CopyExtentDoneResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}


	if !sm.AmLeader() {
		return errDone(wire_errors.NotLeader)
	}

	//client should still have the lock: check lock
	lock := dlock.NewDLock(FormatRecoveryTaskLock(req.ExtentID))
	if err := lock.Lock(100 * time.Millisecond); err == nil {
		lock.Close()
		return errDone(errors.Errorf("client should have lock"))
	}
	lock.Close()

	if req.Invalid == 1 {
		//remove task
		taskKey := FormatRecoveryTaskName(req.ExtentID, req.ReplaceID)
		cmps := []clientv3.Cmp{clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue)}
		ops := []clientv3.Op{
			clientv3.OpDelete(taskKey),
		}	
		if err := manager.EtcdSetKVS(sm.client, cmps, ops); err != nil {
			return errDone(errors.Errorf("setting etcd failed [%s]", err))
		}

		sm.taskPool.Del(taskKey)
		return &pb.CopyExtentDoneResponse{
			Code :pb.Code_OK,
		}, nil
	}


	//valid update
	d, ok := sm.extents.Get(req.ExtentID)
	if !ok {
		errDone(errors.New(""))
	}
	extentInfo := d.(*pb.ExtentInfo)

	//valid extentInfo and replaceID//
	//FIXME:check version as well
	slot := FindReplaceSlot(extentInfo, req.ReplaceID)
	
	if slot == -1 {
		return errDone(errors.Errorf("replaceID is %d, extentInfo is %v", req.ReplaceID, extentInfo))
	}

	//replace ID
	newExtentInfo := proto.Clone(extentInfo).(*pb.ExtentInfo)
	if slot < len(extentInfo.Replicates) {
		newExtentInfo.Replicates[slot] = req.NewNode
	} else {
		newExtentInfo.Parity[slot - len(extentInfo.Replicates)] = req.NewNode
	}

	//EVERSION inc
	newExtentInfo.Eversion++

	//submit newExtentInfo and remove taskPool to etcd

	extentKey := formatExtentKey(req.ExtentID)
	data := utils.MustMarshal(newExtentInfo)

	taskKey := FormatRecoveryTaskName(req.ExtentID, req.ReplaceID)
	cmps := []clientv3.Cmp{clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue)}
	ops := []clientv3.Op{
		clientv3.OpDelete(taskKey),
		clientv3.OpPut(extentKey, string(data)),
	}

	if err := manager.EtcdSetKVS(sm.client, cmps, ops); err != nil {
		return errDone(errors.Errorf("setting etcd failed [%s]", err))
	}

	//remove from taskPool in local memory
	sm.taskPool.Del(taskKey)
	//return successfull
	return &pb.CopyExtentDoneResponse{
		Code :pb.Code_OK,
	}, nil
}
