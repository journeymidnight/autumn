package partition_server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/range_partition"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
)

func (ps *PartitionServer) checkVersion(partID uint64, key []byte) *range_partition.RangePartition {
	ps.RLock()
	rp := ps.rangePartitions[partID]
	ps.RUnlock()
	if rp == nil {
		fmt.Println("no such rp")
		return nil
	}
	if bytes.Compare(rp.StartKey, key) <= 0 && (len(rp.EndKey) == 0 || bytes.Compare(key, rp.EndKey) < 0) {
		return rp
	}
	fmt.Println("compare?")
	return nil
}

func (ps *PartitionServer) Batch(ctx context.Context, req *pspb.BatchRequest) (*pspb.BatchResponse, error) {
	return nil, errors.New("not implemented")
}

func (ps *PartitionServer) Put(ctx context.Context, req *pspb.PutRequest) (*pspb.PutResponse, error) {
	rp := ps.checkVersion(req.Partid, req.Key)
	if rp == nil {
		return nil, errors.New("no such partid")
	}
	if err := rp.Write(req.Key, req.Value); err != nil {
		if err == wire_errors.LockedByOther {
			partID := req.Partid
			defer func(){
				xlog.Logger.Errorf("range partition %s was locked by other ps..close")
				delete(ps.rangePartitions, partID)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				ps.rangePartitionLocks[partID].Unlock(ctx)
				cancel()
				delete(ps.rangePartitionLocks, partID)
			}()
		}
	
		
		return nil, err
	}
	return &pspb.PutResponse{Key: req.Key}, nil

}

func (ps *PartitionServer) Get(ctx context.Context, req *pspb.GetRequest) (*pspb.GetResponse, error) {

	rp := ps.checkVersion(req.Partid, req.Key)
	if rp == nil {
		return nil, errors.New("no such partid")
	}
	v, err := rp.Get(req.Key, 0)
	if err != nil {
		return nil, err
	}
	return &pspb.GetResponse{
		Key:   req.Key,
		Value: v,
	}, nil

}

func (ps *PartitionServer) Delete(ctx context.Context, req *pspb.DeleteRequest) (*pspb.DeleteResponse, error) {
	rp := ps.checkVersion(req.Partid, req.Key)
	if rp == nil {
		return nil, errors.New("no such partid")
	}

	err := rp.Delete(req.Key)
	if err != nil {
		return nil, err
	}

	return &pspb.DeleteResponse{
		Key: req.Key,
	}, nil
}

func (ps *PartitionServer) Range(ctx context.Context, req *pspb.RangeRequest) (*pspb.RangeResponse, error) {
	rp := ps.checkVersion(req.Partid, req.Start)
	if rp == nil {
		return nil, errors.New("no such partid")
	}
	out := rp.Range(req.Prefix, req.Start, req.Limit)
	return &pspb.RangeResponse{
		Truncated: 0,
		Keys:      out,
	}, nil
}
