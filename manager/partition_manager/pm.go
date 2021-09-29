package partition_manager

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
)

const (
	pmElectionKeyPrefix = "AutumnPMLeader"
)

//FIXME: 考虑重新分配range partitin的情况, 可能把程序变成单线程server, 避免过多的lock
type PartitionManager struct {
	etcd       *embed.Etcd
	client     *clientv3.Client
	grcpServer *grpc.Server
	ID         uint64 //backend ETCD server's ID

	isLeader    int32
	memberValue string
	//leadeKey is to store Election key
	leaderKey string
	config    *manager.Config

	//pslock  utils.SafeMutex           //protect
	psNodes map[uint64]*pspb.PSDetail //cache from etcd, read lasted PSNodes when became leader

	//partLock utils.SafeMutex
	partMeta map[uint64]*pspb.PartitionMeta

	currentRegions *pspb.Regions
	//allocIdLock utils.SafeMutex

	policy  AllocPartPolicy
	stopper *utils.Stopper //all pm logic in this stopper's routine

	utils.SafeMutex
}

type AllocPartPolicy interface {
	AllocPart(nodes map[uint64]*pspb.PSDetail) (PSID uint64, err error)
}

func NewPartitionManager(etcd *embed.Etcd, client *clientv3.Client, config *manager.Config) *PartitionManager {
	pm := &PartitionManager{
		etcd:    etcd,
		client:  client,
		config:  config,
		ID:      uint64(etcd.Server.ID()),
		policy:  SimplePolicy{},
		stopper: utils.NewStopper(),
	}

	v := pb.MemberValue{
		ID:      pm.ID,
		Name:    etcd.Config().Name + "_PM",
		GrpcURL: config.GrpcUrl,
	}

	data, err := v.Marshal()
	utils.Check(err)

	pm.memberValue = string(data)

	return pm

}

func (pm *PartitionManager) AmLeader() bool {
	return atomic.LoadInt32(&pm.isLeader) == 1
}

var parseError = errors.New("parse error")

func parseParts(kvs []*mvccpb.KeyValue) map[uint64]*pspb.PartitionMeta {
	ret := make(map[uint64]*pspb.PartitionMeta)
	for _, kv := range kvs {
		var meta pspb.PartitionMeta
		if err := meta.Unmarshal(kv.Value); err != nil {
			xlog.Logger.Warnf(err.Error())
			continue
		}
		ret[meta.PartID] = &meta
	}
	return ret
}

func (pm *PartitionManager) watchPSEvents(watchServerCh clientv3.WatchChan, closeWatch func()) {
	var err error
	for {
		select {
		case <-pm.stopper.ShouldStop():
			pm.Lock()
			closeWatch()
			pm.currentRegions = nil
			pm.psNodes = nil
			pm.Unlock()
			return
		case res := <-watchServerCh:
			pm.Lock()
			for _, e := range res.Events {
				var psDetail pspb.PSDetail
				switch e.Type.String() {
				case "PUT":
					if err = psDetail.Unmarshal(e.Kv.Value); err != nil {
						break
					}
					//pm.pslock.Lock()
					pm.psNodes[psDetail.PSID] = &psDetail
					//pm.pslock.Unlock()

					//if there is any PART who is not allocated, allocated to psDetail.PSID
					var anyChange bool

					//pm.partLock.RLock()
					for _, part := range pm.partMeta {
						_, ok := pm.currentRegions.Regions[part.PartID]
						if !ok {
							pm.currentRegions.Regions[part.PartID] = &pspb.RegionInfo{
								Rg:     part.Rg,
								PartID: part.PartID,
								PSID:   psDetail.PSID,
							}
							anyChange = true
						}
					}
					//pm.partLock.RUnlock()

					if anyChange {
						fmt.Printf("NEW PS: set regions/config %+v", pm.currentRegions.Regions)
						ops := []clientv3.Op{
							clientv3.OpPut(fmt.Sprintf("regions/config"), string(utils.MustMarshal(pm.currentRegions))),
						}
						if err = etcd_utils.EtcdSetKVS(pm.client, []clientv3.Cmp{
							clientv3.Compare(clientv3.Value(pm.leaderKey), "=", pm.memberValue),
						}, ops); err != nil {
							xlog.Logger.Warnf("this pm is not leader , can not set 'regions/config'")
						}
					} else {
						fmt.Printf("NEW PS: nothing changed\n")
					}

				case "DELETE":
					fmt.Printf("DELETE msg %+v", e)
					if err = psDetail.Unmarshal(e.PrevKv.Value); err != nil {
						break
					}
					//pm.pslock.Lock()
					delete(pm.psNodes, psDetail.PSID)
					//pm.pslock.Unlock()

					//change regions
					//move partition [partID] from psDetail.ps to "to"
					type MovePartition struct {
						partID uint64
						to     uint64
					}

					var moves []MovePartition
					for _, region := range pm.currentRegions.Regions {
						if region.PSID == psDetail.PSID {
							//move region.PartID to other ps
							psID, err := pm.policy.AllocPart(pm.psNodes)
							if err != nil {
								xlog.Logger.Errorf("can not alloc parts to partition servers")
								//set error somewhere
								//will set region's PSID = 0
								delete(pm.currentRegions.Regions, region.PartID)
							} else {
								moves = append(moves, MovePartition{partID: region.PartID, to: psID})
							}
						}
					}

					//先clone pm.currentRegions, 然后setETCD, 再更新pm.currentRegions更好
					//但是这样写更加简单, 并且只会导致监控看region数据时, 有可能有错误数据
					for _, move := range moves {
						pm.currentRegions.Regions[move.partID].PSID = move.to
					}
					data := utils.MustMarshal(pm.currentRegions)

					//set etcd
					fmt.Printf("DELETE PS: set regions/config %+v", pm.currentRegions.Regions)
					ops := []clientv3.Op{
						clientv3.OpPut(fmt.Sprintf("regions/config"), string(data)),
					}
					if err = etcd_utils.EtcdSetKVS(pm.client, []clientv3.Cmp{
						clientv3.Compare(clientv3.Value(pm.leaderKey), "=", pm.memberValue),
					}, ops); err != nil {
						xlog.Logger.Warnf("this pm is not leader , can not set 'regions/config'")
					}
				}
			}
			pm.Unlock()
		}
	}
}

func (pm *PartitionManager) runAsLeader() {
	//pm.pslock.Lock()
	//defer pm.pslock.Unlock()

	//pm.partLock.Lock()
	//defer pm.partLock.Unlock()
	//load data

	var maxRev int64
	var rev int64
	kvs, rev, err := etcd_utils.EtcdRange(pm.client, "PSSERVER")
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return
	}
	maxRev = utils.Max64(rev, maxRev)

	pm.psNodes = make(map[uint64]*pspb.PSDetail)
	for _, kv := range kvs {
		psid, err := parseKey(string(kv.Key), "PSSERVER")
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		var detail pspb.PSDetail
		if err = detail.Unmarshal(kv.Value); err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		pm.psNodes[psid] = &detail
	}

	fmt.Printf("psNodes is %+v\n", pm.psNodes)

	kvs, rev, err = etcd_utils.EtcdRange(pm.client, "PART/")
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return
	}
	maxRev = utils.Max64(rev, maxRev)

	pm.partMeta = parseParts(kvs)

	//is current config valid?
	var data []byte
	data, rev, err = etcd_utils.EtcdGetKV(pm.client, "regions/config")

	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return
	}

	maxRev = utils.Max64(rev, maxRev)

	var regions pspb.Regions
	utils.MustUnMarshal(data, &regions)

	for _, region := range regions.Regions {
		part := pm.partMeta[region.PartID]
		if part == nil {
			//在删除part时, 同时也会修config, 不应当出现这个情况, BUGON
			//如果用其他方式如从etcd删除part, 可以直接删除"regions/config"
			xlog.Logger.Fatal("part %d is in config, but not int meta", region.PartID)
		}
	}

	if len(regions.Regions) == 0 {
		regions.Regions = make(map[uint64]*pspb.RegionInfo)
	}

	partsAlloc := make([]uint64, 0, 10)
	for partID := range pm.partMeta {
		utils.AssertTrue(partID != 0)
		regionInfo, ok := regions.Regions[partID]
		if !ok {
			//part没有在config里面分配, 记录下来, 准备修改
			partsAlloc = append(partsAlloc, partID)
			continue
		}
		//part在config和meta里面都存在, 检查对应的ps是否存在

		if _, ok := pm.psNodes[regionInfo.PSID]; !ok {
			xlog.Logger.Warnf("part %d is alloced on ps %d, but ps %d is down, realloc", partID, regionInfo.PSID, regionInfo.PSID)
			delete(regions.Regions, partID)
			//也重新分配
			partsAlloc = append(partsAlloc, partID)
		}
	}

	//重新分配partsAlloc
	for _, partID := range partsAlloc {
		//已知psNodes状态,
		//clone pm.psNodes
		psID, err := pm.policy.AllocPart(pm.psNodes)
		if err != nil {
			//分配错误?
			xlog.Logger.Errorf("can not alloc parts to partition servers")
			continue
		}
		regions.Regions[partID] = &pspb.RegionInfo{
			Rg:     pm.partMeta[partID].Rg,
			PartID: partID,
			PSID:   psID,
		}
	}

	//if regions changed, set "config"
	if len(partsAlloc) > 0 {
		fmt.Printf("PM: set regions/config:\n")
		for _, v := range regions.Regions {
			fmt.Printf("partID: %d , rg is %v is on ps %d\n", v.PartID, v.Rg, v.PSID)
		}
		ops := []clientv3.Op{
			clientv3.OpPut(fmt.Sprintf("regions/config"), string(utils.MustMarshal(&regions))),
		}
		err = etcd_utils.EtcdSetKVS(pm.client, []clientv3.Cmp{
			clientv3.Compare(clientv3.Value(pm.leaderKey), "=", pm.memberValue),
		}, ops)
		if err != nil {
			xlog.Logger.Warnf("this pm is not leader , can not set 'regions/config'")
			return
		}
	} else {
		fmt.Printf("PM: regions/config remain the same %+v\n", regions.Regions)

	}
	pm.currentRegions = &regions

	//start watch PSSERVER
	watchPSServerCh, closeWatchPSCh := etcd_utils.EtcdWatchEvents(pm.client, "PSSERVER/", "PSSERVER0", maxRev)
	watchPartServerCh, closeWatchPartCh := etcd_utils.EtcdWatchEvents(pm.client, "PART/", "PART0", maxRev)

	pm.stopper.RunWorker(func() {
		pm.watchPSEvents(watchPSServerCh, closeWatchPSCh)
	})

	pm.stopper.RunWorker(func() {
		pm.watchPartEvents(watchPartServerCh, closeWatchPartCh)
	})
	atomic.StoreInt32(&pm.isLeader, 1)
}

func (pm *PartitionManager) etcdLeader() uint64 {
	return uint64(pm.etcd.Server.Leader())
}

//watchPartEvents watch part events
func (pm *PartitionManager) watchPartEvents(watchCh clientv3.WatchChan, closeCh func()) {
	var err error
	for {
		select {
		case <-pm.stopper.ShouldStop():
			closeCh()
			pm.Lock()
			pm.partMeta = nil
			pm.Unlock()
			return
		case event := <-watchCh:
			pm.Lock()
			for _, e := range event.Events {
				var partMeta pspb.PartitionMeta
				switch e.Type.String() {
				case "PUT":
					utils.MustUnMarshal(e.Kv.Value, &partMeta)
					var psID uint64
					if _, ok := pm.partMeta[partMeta.PartID]; ok {
						fmt.Printf("old part %d is updated [%v]\n", partMeta.PartID, partMeta.Rg)
					} else {
						fmt.Printf("new part %d is created [%v]\n", partMeta.PartID, partMeta.Rg)
					}
					pm.partMeta[partMeta.PartID] = &partMeta

					//part是否在regions/config里面?
					if regionInfo, ok := pm.currentRegions.Regions[partMeta.PartID]; ok {
						psID = regionInfo.PSID //当split发生时, 保持原来的PartID位置
					} else {
						//alloc partMeta
						psID, err = pm.policy.AllocPart(pm.psNodes)
						if err != nil {
							//分配错误?
							xlog.Logger.Errorf("can not alloc parts to partition servers")
							continue
						}
					}

					pm.currentRegions.Regions[partMeta.PartID] = &pspb.RegionInfo{
						Rg:     partMeta.Rg,
						PartID: partMeta.PartID,
						PSID:   psID,
					}
					fmt.Printf("PART Changed: set regions/config %+v", pm.currentRegions.Regions)

					ops := []clientv3.Op{
						clientv3.OpPut(fmt.Sprintf("regions/config"), string(utils.MustMarshal(pm.currentRegions))),
					}
					if err = etcd_utils.EtcdSetKVS(pm.client, []clientv3.Cmp{
						clientv3.Compare(clientv3.Value(pm.leaderKey), "=", pm.memberValue),
					}, ops); err != nil {
						xlog.Logger.Warnf("this pm is not leader , can not set 'regions/config'")
					}
				case "DELETE":
					xlog.Logger.Warnf("partition %d is deleted", e.Kv.Key)
				default:
					//in case of "DELETE" or others
					xlog.Logger.Fatalf("no solution for event %s", e.Type.String())
				}
			}
			pm.Unlock()
		}
	}
}

//FIXME:put into etcd_op
func (pm *PartitionManager) LeaderLoop() {
	for {
		if pm.ID != pm.etcdLeader() {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		s, err := concurrency.NewSession(pm.client, concurrency.WithTTL(15))
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			continue
		}
		//returns a new election on a given key prefix
		e := concurrency.NewElection(s, pmElectionKeyPrefix)
		ctx := context.TODO()

		if err = e.Campaign(ctx, pm.memberValue); err != nil {
			xlog.Logger.Warnf(err.Error())
			continue
		}
		pm.leaderKey = e.Key()
		xlog.Logger.Infof("elected %d as leader", pm.ID)
		pm.runAsLeader()

		select {
		case <-s.Done():
			atomic.StoreInt32(&pm.isLeader, 0)
			s.Close()
			pm.stopper.Stop()
			xlog.Logger.Info("%d's leadershipt expire", pm.ID)
		}
	}
}

//FIXME
func (pm *PartitionManager) Close() {

}

func parseKey(s string, prefix string) (uint64, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return 0, errors.Errorf("parse key[%s] failed :", s)
	}
	if parts[0] != prefix {
		return 0, errors.Errorf("parse key[%s] failed, parts[0] not match :", s)
	}
	return strconv.ParseUint(parts[1], 10, 64)
}
