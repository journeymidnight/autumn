package smclient

import (
	"context"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"google.golang.org/grpc"
)

type ExtentManager struct {
	sync.RWMutex
	smClient   *SMClient
	extentInfo map[uint64]*pb.ExtentInfo
}

func NewExtentManager(sm *SMClient) *ExtentManager {
	return &ExtentManager{
		smClient:   sm,
		extentInfo: make(map[uint64]*pb.ExtentInfo),
	}
}

func (em *ExtentManager) GetPeers(extentID uint64) []string {

	extentInfo := em.GetExtentInfo(extentID)

	var ret []string
	for _, id := range extentInfo.Replicates {
		n := em.smClient.LookupNode(id)
		utils.AssertTrue(n != nil)
		ret = append(ret, n.Address)
	}
	for _, id := range extentInfo.Parity {
		n := em.smClient.LookupNode(id)
		utils.AssertTrue(n != nil)
		ret = append(ret, n.Address)
	}
	return ret
}

//always get alive node: FIXME: make sure pool healthy
func (em *ExtentManager) GetExtentConn(extentID uint64) *grpc.ClientConn {
	/*
	extentInfo := em.GetExtentInfo(extentID)
	nodeInfo := em.smClient.LookupNode(extentInfo.Replicates[0])
	pool := conn.GetPools().Connect(nodeInfo.Address)
	*/
	peerAddrs := em.GetPeers(extentID)
	if len(peerAddrs) == 0 {
		return nil
	}
	for i := range peerAddrs {
		pool := conn.GetPools().Connect(peerAddrs[i])
		if pool != nil && pool.IsHealthy() {
			return pool.Get()
		}
	}
	return nil
}

func (em *ExtentManager) Update(extentID uint64) *pb.ExtentInfo{
	var ei *pb.ExtentInfo
		//loop forever
	m, err := em.smClient.ExtentInfo(context.Background(), []uint64{extentID})
	if err != nil {
		return nil
	}
	ei = proto.Clone(m[extentID]).(*pb.ExtentInfo)	
	em.Lock()
	em.extentInfo[extentID] = ei
	em.Unlock()
	return ei
}

func (em *ExtentManager) GetExtentInfo(extentID uint64) *pb.ExtentInfo {
	em.RLock()
	info, ok := em.extentInfo[extentID]
	em.RUnlock()
	if !ok {
		info = em.Update(extentID)
	}
	return info
}

func (em *ExtentManager) SetExtentInfo(extentID uint64, info *pb.ExtentInfo) {
	em.Lock()
	defer em.Unlock()
	em.extentInfo[extentID] = info
}