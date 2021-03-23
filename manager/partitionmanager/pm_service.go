package partitionmanager

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

func (pm *PartitionManager) GetPartitionMeta(ctx context.Context, req *pspb.GetPartitionMetaRequest) (*pspb.GetPartitionMetaResponse, error) {

	if !pm.AmLeader() {
		return &pspb.GetPartitionMetaResponse{
			Code: pb.Code_NOT_LEADER,
		}, nil
	}

	pm.partLock.RLock()
	defer pm.partLock.RUnlock()
	var ret []*pspb.PartitionMeta
	for _, meta := range pm.partMeta {
		if meta.Parent == req.PSID {
			ret = append(ret, proto.Clone(meta).(*pspb.PartitionMeta))
		}
	}
	return &pspb.GetPartitionMetaResponse{
		Code: pb.Code_OK,
		Meta: ret,
	}, nil
}

//FIXME:ERROR
func (pm *PartitionManager) SetRowStreamTables(ctx context.Context, req *pspb.SetRowStreamTablesRequest) (*pspb.SetRowStreamTablesResponse, error) {
	done := func(err error) (*pspb.SetRowStreamTablesResponse, error) {
		return &pspb.SetRowStreamTablesResponse{
			Code: pb.Code_ERROR,
		}, nil
	}

	if !pm.AmLeader() {
		return done(errors.Errorf("not a leader"))
	}

	if len(req.Locs.Locs) == 0 || req.PartitionID == 0 {
		return done(errors.Errorf("invalid request"))
	}

	data, err := req.Locs.Marshal()

	ops := []clientv3.Op{
		clientv3.OpPut(fmt.Sprintf("PART/%d/tables", req.PartitionID), string(data)),
	}

	err = manager.EtctSetKVS(pm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(pm.leaderKey), "=", pm.memberValue),
	}, ops)

	if err != nil {
		return done(err)
	}

	pm.partLock.Lock()

	pm.partMeta[req.PartitionID].Locs = proto.Clone(req.Locs).(*pspb.TableLocations)
	pm.partLock.Unlock()

	return &pspb.SetRowStreamTablesResponse{
		Code: pb.Code_OK,
	}, nil

}

func (pm *PartitionManager) allocUniqID(count uint64) (uint64, uint64, error) {

	pm.allocIdLock.Lock()
	defer pm.allocIdLock.Unlock()
	return manager.EtcdAllocUniqID(pm.client, idKey, count)
}

func (pm *PartitionManager) RegisterPS(ctx context.Context, req *pspb.RegisterPSRequest) (*pspb.RegisterPSResponse, error) {
	if !pm.AmLeader() {
		return nil, errors.Errorf("not a leader")
	}

	if len(req.Addr) == 0 {
		return nil, errors.Errorf("no address")
	}
	var hasDup bool
	pm.pslock.RLock()
	for _, n := range pm.psNodes {
		if n.Address == req.Addr {
			break
		}
	}
	pm.pslock.RUnlock()

	if hasDup {
		return &pspb.RegisterPSResponse{
			Code: pb.Code_ERROR,
		}, nil
	}

	id, _, err := pm.allocUniqID(1)
	if err != nil {
		return nil, errors.Errorf("failed to alloc uniq id")
	}

	psDetail := &pspb.PSDetail{
		PSID:    id,
		Address: req.Addr,
	}
	//modify etcd

	data, err := psDetail.Marshal()
	utils.Check(err)
	PSKEY := fmt.Sprintf("PSSERVER/%d", id)
	ops := []clientv3.Op{
		clientv3.OpPut(PSKEY, string(data)),
	}

	err = manager.EtctSetKVS(pm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(pm.leaderKey), "=", pm.memberValue),
	}, ops)
	if err != nil {
		return nil, err
	}

	pm.pslock.Lock()
	defer pm.pslock.Unlock()

	pm.psNodes[id] =
		&pspb.PSDetail{
			PSID:    id,
			Address: req.Addr,
		}

	return &pspb.RegisterPSResponse{
		Code: pb.Code_OK,
		Id:   id,
	}, nil
}

func (pm *PartitionManager) GetPSInfo(ctx context.Context, req *pspb.GetPSInfoRequest) (*pspb.GetPSInfoResponse, error) {
	if !pm.AmLeader() {
		return nil, errors.Errorf("not a leader")
	}
	var ret []*pspb.PSDetail
	pm.pslock.RLock()
	for _, ps := range pm.psNodes {
		ret = append(ret, ps)
	}
	pm.pslock.RUnlock()

	return &pspb.GetPSInfoResponse{
		Servers: ret,
	}, nil
}

func uint64ToBig(x uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], x)
	return string(b[:])
}

func (pm *PartitionManager) Bootstrap(ctx context.Context, req *pspb.BootstrapRequest) (*pspb.BootstrapResponse, error) {
	if !pm.AmLeader() {
		return nil, errors.Errorf("not a leader")
	}

	//alloc new partID
	partID, _, err := pm.allocUniqID(2)
	if err != nil {
		return nil, err
	}

	pm.partLock.Lock()
	defer pm.partLock.Unlock()

	//FIXME: check req

	rg := &pspb.Range{StartKey: []byte(""), EndKey: []byte("")}
	rangeValue, err := rg.Marshal()
	if err != nil {
		return nil, err
	}
	ops := []clientv3.Op{
		clientv3.OpPut(fmt.Sprintf("PART/%d/logStream", partID), uint64ToBig(req.LogID)),
		clientv3.OpPut(fmt.Sprintf("PART/%d/rowStream", partID), uint64ToBig(req.RowID)),
		clientv3.OpPut(fmt.Sprintf("PART/%d/parent", partID), uint64ToBig(req.Parent)),
		clientv3.OpPut(fmt.Sprintf("PART/%d/range", partID), string(rangeValue)),
	}

	//FIXME: update PSVERSION
	err = manager.EtctSetKVS(pm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(pm.leaderKey), "=", pm.memberValue),
	}, ops)

	pm.partMeta[partID] = &pspb.PartitionMeta{
		LogStream: req.LogID,
		RowStream: req.RowID,
		Parent:    req.Parent,
		Rg:        rg,
		PartID:    partID,
	}
	return &pspb.BootstrapResponse{PartID: partID}, nil
}

func (pm *PartitionManager) GetRegions(ctx context.Context, req *pspb.GetRegionsRequest) (*pspb.GetRegionsResponse, error) {

	done := func(err error) (*pspb.GetRegionsResponse, error) {
		return &pspb.GetRegionsResponse{
			Code: pb.Code_ERROR,
		}, nil
	}

	if !pm.AmLeader() {
		return done(errors.Errorf("not a leader"))
	}

	pm.partLock.RLock()
	defer pm.partLock.RUnlock()
	/*
		pm.pslock.RLock()
		defer pm.pslock.RUnlock()
	*/
	var regions []*pspb.RegionInfo
	for partID, meta := range pm.partMeta {
		utils.AssertTrue(meta.Rg != nil)
		region := &pspb.RegionInfo{
			Rg:     meta.Rg,
			PartID: partID,
			PSID:   meta.Parent,
		}
		pm.pslock.RLock()
		detail, ok := pm.psNodes[meta.Parent]
		if !ok {
			xlog.Logger.Warnf("can not find region's address")

			pm.pslock.RUnlock()
			continue
		}
		pm.pslock.RUnlock()
		region.Addr = detail.Address
		fmt.Printf("return region is %+v\n", region)
		regions = append(regions, region)
	}

	return &pspb.GetRegionsResponse{
		Code:    pb.Code_OK,
		Regions: regions,
	}, nil

}
