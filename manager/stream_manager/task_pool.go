package stream_manager

import (
	"github.com/cornelk/hashmap"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
)

//import "github.com/cornelk/hashmap"
//or use go-memdb as multiIndex
type TaskPool struct {
	extentMap *hashmap.HashMap //primary key is extentID: extentID=>recoveryTask
	nodeMap   *hashmap.HashMap //nodeID=>[hashmap of recoveryTask]
	utils.SafeMutex
}


func NewTaskPool() *TaskPool {
	return &TaskPool{
		extentMap: &hashmap.HashMap{},
		nodeMap: &hashmap.HashMap{},
	}
}


func (tp *TaskPool) Remove(extentID uint64) {
	tp.Lock()
	defer tp.Unlock()

	d, ok := tp.extentMap.Get(extentID)
	if !ok {
		return
	}
	tp.extentMap.Del(extentID)

	t := d.(*pb.RecoveryTask)

	d, ok = tp.nodeMap.Get(t.NodeID)
	if !ok {
		return
	}
	tl := d.(*hashmap.HashMap)
	tl.Del(t.NodeID)
	
}

func (tp *TaskPool) Insert(t *pb.RecoveryTask) {

	tp.Lock()
	defer tp.Unlock()
	extentID := t.ExtentID
	tp.extentMap.Set(extentID, t)

	d, ok := tp.nodeMap.Get(t.NodeID)
	if ok {
		tl := d.(*hashmap.HashMap)
		tl.Set(extentID, t)
	} else {
		tl := &hashmap.HashMap{}
		tl.Set(t.NodeID, t)
		tp.nodeMap.Set(t.NodeID, tl)
	}
}


func (tp *TaskPool) GetFromNode(nodeID uint64) []*pb.RecoveryTask {
	tp.RLock()
	defer tp.RUnlock()

	d, ok := tp.nodeMap.Get(nodeID)
	if !ok {
		return nil
	}

	tl := d.(*hashmap.HashMap)
	ret := make([]*pb.RecoveryTask, tl.Len())
	i := 0
	for kv := range tl.Iter()  {
		ret[i] = kv.Value.(*pb.RecoveryTask)
		i ++
	}
	return ret
}


func (tp *TaskPool) GetFromExtent(extentID uint64) *pb.RecoveryTask {
	tp.RLock()
	defer tp.RUnlock()

	d, ok := tp.extentMap.Get(extentID)
	if !ok {
		return nil
	}
	ret := d.(*pb.RecoveryTask)
	return ret
}

func (tp *TaskPool) HasTask(extentID uint64) bool {
	tp.RLock()
	defer tp.RUnlock()

	_, ok := tp.extentMap.Get(extentID)
	return ok
}
