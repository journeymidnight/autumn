package stream_manager

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
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



func formatRecoveryTaskName(rt *pb.RecoveryTask) string {
	return fmt.Sprintf("%d_%d.tsk", rt.ExtentID, rt.ReplaceID)
}


//non-block
func (sm *StreamManager) queueRecoveryTask(extentID uint64, replaceNodeID uint64) {
	go func() {
		rt := &pb.RecoveryTask{
			ExtentID: extentID,
			ReplaceID: replaceNodeID,
		}
		key := formatRecoveryTaskName(rt)

		//deduplicate
		if _, ok := sm.taskPool.Get(key); ok {
			return
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
			return
		}
		sm.taskPool.Set(key, &RecoveryTask{
			task: rt,
		})
	}()
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
				sm.taskLock.RLock()
				for kv := range sm.extents.Iter() {
					extent := kv.Value.(*pb.ExtentInfo) //extent is read only
					for _, nodeID := range extent.Replicates {
						ns := sm.getNodeStatus(nodeID)
						if ns == nil || ns.Dead() {
							sm.queueRecoveryTask(extent.ExtentID, nodeID)
						}
					}

					for _, nodeID := range extent.Parity {
						ns := sm.getNodeStatus(nodeID)
						if ns == nil || ns.Dead() {
							sm.queueRecoveryTask(extent.ExtentID, nodeID)
						}
					}
					
				}
				sm.taskLock.RUnlock()
		}
	}
	
}

//non-block
func (sm *StreamManager) submitRecoveryTask(t *RecoveryTask) {
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
			xlog.Logger.Warnf("submitRecoveryTask can not connect %v", chosenNode)
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
			xlog.Logger.Warnf("submitRecoveryTask %s", err.Error())
			return
		}
		//logic error
		if res.Code != pb.Code_OK {
			xlog.Logger.Warnf("submitRecoveryTask %s", res.CodeDes)
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
				sm.taskLock.RLock()
				for kv := range sm.taskPool.Iter() {
					t := kv.Value.(*RecoveryTask)
					//FIXME, if no runningNode and startTime is long ago, retry it
					if t.RunningNode() == 0 && time.Now().Sub(t.StartTime()) > 30 * time.Second {
						sm.submitRecoveryTask(t)
					}
				}
				sm.taskLock.RUnlock()
			}
	}
}