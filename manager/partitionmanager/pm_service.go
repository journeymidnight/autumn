package partitionmanager

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/pkg/errors"
)


func (pm *PartitionManager) allocUniqID(count uint64) (uint64, uint64, error) {
	return etcd_utils.EtcdAllocUniqID(pm.client, idKey, count)
}

func (pm *PartitionManager) Bootstrap(ctx context.Context, req *pspb.BootstrapRequest) (*pspb.BootstrapResponse, error) {

	errDone := func(err error) (*pspb.BootstrapResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pspb.BootstrapResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}
	if !pm.AmLeader() {
		return errDone(wire_errors.NotLeader)
	}

	//watch started and no partitions

	pm.Lock()
	defer pm.Unlock()

	if len(pm.partMeta) > 0 {
		return errDone(errors.New("already has partMeta, can not bootstrap"))
	}

	//alloc new partID
	partID, _, err := pm.allocUniqID(2)
	if err != nil {
		return nil, err
	}

	//FIXME: check request	
	zeroMeta := pspb.PartitionMeta{
		LogStream: req.LogID,
		RowStream: req.RowID,
		Rg:&pspb.Range{StartKey: []byte(""), EndKey: []byte("")},
		PartID: partID,
	}
	
	
	ops := []clientv3.Op{
		clientv3.OpPut(fmt.Sprintf("PART/%d", partID), string(utils.MustMarshal(&zeroMeta))),
	}

	err = etcd_utils.EtcdSetKVS(pm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(pm.leaderKey), "=", pm.memberValue),
	}, ops)

	pm.partMeta[partID] = &zeroMeta

	return &pspb.BootstrapResponse{PartID: partID, Code: pb.Code_OK}, nil
}