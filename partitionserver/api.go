package partitionserver

import (
	"bytes"
	"context"
	"errors"

	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/rangepartition"
)

//FIXME: inc and decr
func (ps *PartitionServer) checkVersion(verison uint64, partID uint64, key []byte) *rangepartition.RangePartition {
	ps.RLock()
	rp := ps.rangePartitions[partID]
	ps.RUnlock()
	if rp == nil {
		return nil
	}
	if bytes.Compare(rp.StartKey, key) <= 0 && (len(rp.EndKey) == 0 || bytes.Compare(key, rp.EndKey) < 0) {
		return rp
	}
	return nil
}

func (ps *PartitionServer) Batch(ctx context.Context, req *pspb.BatchRequest) (*pspb.BatchResponse, error) {
	return nil, errors.New("not implemented")
}

func (ps *PartitionServer) Put(ctx context.Context, req *pspb.PutRequest) (*pspb.PutResponse, error) {
	rp := ps.checkVersion(req.Psversion, ps.PSID, req.Key)
	if rp == nil {
		return nil, errors.New("no such partid")
	}
	if err := rp.Write(req.Key, req.Value); err != nil {
		return nil, err
	}
	return &pspb.PutResponse{Key: req.Key}, nil

}

func (ps *PartitionServer) Get(ctx context.Context, req *pspb.GetRequest) (*pspb.GetResponse, error) {

	rp := ps.checkVersion(req.Psversion, req.Partid, req.Key)
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
	return nil, errors.New("not implemented")
}
