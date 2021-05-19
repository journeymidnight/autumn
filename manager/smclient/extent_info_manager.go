package smclient

import (
	"context"
	"math/rand"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"google.golang.org/grpc"
)

//TODO: LRU
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


type SelectNodePolicy interface{
	Choose(replics []string) *grpc.ClientConn   
}

type PrimaryPolicy struct{}
func (PrimaryPolicy) Choose(replics []string) *grpc.ClientConn {
	addr := replics[0]
	pool, err := conn.GetPools().Get(addr)
	if err != nil {
		return nil
	}
	return pool.Get()

}

type RandomPolicy struct{}
func (RandomPolicy) Choose(replics []string) *grpc.ClientConn{
	for loop := 0 ; loop < 3 ; loop ++{
		n := rand.Intn(len(replics))
		pool, err := conn.GetPools().Get(replics[n])
		if err == nil {
			continue
		}
		return pool.Get()
	}
	return nil
}

func (em *ExtentManager) GetExtentConn(extentID uint64, policy SelectNodePolicy) *grpc.ClientConn {

	peerAddrs := em.GetPeers(extentID)
	if len(peerAddrs) == 0 {
		return nil
	}
	return policy.Choose(peerAddrs)	
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