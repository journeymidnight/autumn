package partition_server

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"

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
		fmt.Printf("no such rp %d\n", partID)
		return nil
	}
	if rp.IsUserKeyInRange(key) {
		return rp
	}
	fmt.Println("key is not in range")
	return nil
}

func (ps *PartitionServer) Batch(ctx context.Context, req *pspb.BatchRequest) (*pspb.BatchResponse, error) {
	return nil, errors.New("not implemented")
}

func (ps *PartitionServer) StreamPut(stream pspb.PartitionKV_StreamPutServer) error {

	errDone := func(err error) error {
		key := fmt.Sprintf("err: %s", err)
		stream.SendAndClose(&pspb.PutResponse{
			Key: []byte(key),
		})
		return nil
	}

	req, err := stream.Recv()
	if err != nil {
		return err
	}

	header := req.GetHeader()

	rp := ps.checkVersion(header.Partid, header.GetKey())
	if rp == nil {
		return errDone(errors.New("no such partid"))
	}

	entry := range_partition.NewPutEntry(header.Key, header.ExpiresAt, header.LenOfValue)
	//received at most header.LenOfValue bytes
	for {
		req, err := stream.Recv()
		if err != nil && err != io.EOF {
			return errDone(err)
		}
		if req.GetPayload() == nil {
			break
		}
		if err = entry.WriteValue(req.GetPayload()); err != nil {
			//payload is too large
			return errDone(err)
		}
	}
	entry.FinishWrite()

	//valid uploaded size
	if len(entry.Value) != int(header.LenOfValue) {
		return errDone(errors.Errorf("payload is %s, header.LenOfValue is %d", len(entry.Value), header.LenOfValue))
	}

	if err = rp.WriteEntries([]*range_partition.Entry{entry}); err != nil {
		if err == wire_errors.LockedByOther {
			partID := header.Partid
			defer func() {
				xlog.Logger.Errorf("range partition %s was locked by other ps..close")
				delete(ps.rangePartitions, partID)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				ps.rangePartitionLocks[partID].Unlock(ctx)
				cancel()
				delete(ps.rangePartitionLocks, partID)
			}()
		}

		return errDone(err)
	}

	return stream.SendAndClose(&pspb.PutResponse{
		Key: []byte(header.Key),
	})
}

func (ps *PartitionServer) Put(ctx context.Context, req *pspb.PutRequest) (*pspb.PutResponse, error) {
	rp := ps.checkVersion(req.Partid, req.Key)
	if rp == nil {
		return nil, errors.New("no such partid")
	}
	if err := rp.Write(req.Key, req.Value); err != nil {
		if err == wire_errors.LockedByOther {
			partID := req.Partid
			defer func() {
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

func (ps *PartitionServer) Head(ctx context.Context, req *pspb.HeadRequest) (*pspb.HeadResponse, error) {
	rp := ps.checkVersion(req.Partid, req.Key)
	if rp == nil {
		return nil, errors.New("no such partid")
	}
	info, err := rp.Head(req.Key)
	if err != nil {
		return nil, err
	}

	return &pspb.HeadResponse{
		Info: info,
	}, nil
}

func (ps *PartitionServer) Get(ctx context.Context, req *pspb.GetRequest) (*pspb.GetResponse, error) {

	rp := ps.checkVersion(req.Partid, req.Key)
	if rp == nil {
		return nil, errors.New("no such partid")
	}

	v, err := rp.Get(req.Key)
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
	ps.RLock()
	rp := ps.rangePartitions[req.Partid]
	ps.RUnlock()
	if rp == nil {
		return nil, errors.New("no such partid")
	}

	out := rp.Range(req.Prefix, req.Start, req.Limit)

	var truncated bool
	if len(out) == int(req.Limit) {
		truncated = true
	} else {
		truncated = false
	}
	return &pspb.RangeResponse{
		Truncated: truncated,
		Keys:      out,
	}, nil
}

func (ps *PartitionServer) Maintenance(ctx context.Context, req *pspb.MaintenanceRequest) (*pspb.MaintenanceResponse, error) {
	ps.RLock()
	rp := ps.rangePartitions[req.Partid]
	ps.RUnlock()
	if rp == nil {
		fmt.Printf("no such rp %d\n", req.Partid)
		return nil, errors.New("no such partid")
	}

	var err error
	switch t := req.OP.(type) {
	case *pspb.MaintenanceRequest_Compact:
		err = rp.SubmitCompaction()
	case *pspb.MaintenanceRequest_Autogc:
		err = rp.SubmitGC(range_partition.GcTask{})
	case *pspb.MaintenanceRequest_Forcegc:
		err = rp.SubmitGC(range_partition.GcTask{
			ForceGC: true,
			ExIDs:   t.Forcegc.ExIDs,
		})
	default:
		err = errors.New("unknown op")
	}
	return &pspb.MaintenanceResponse{}, err
}

func (ps *PartitionServer) SplitPart(ctx context.Context, req *pspb.SplitPartRequest) (*pspb.SplitPartResponse, error) {
	ps.RLock()
	rp := ps.rangePartitions[req.Partid]
	ps.RUnlock()
	if rp == nil {
		fmt.Printf("no such rp %d\n", req.Partid)
		return nil, errors.New("no such partid")
	}

	if !rp.CanSplit() {
		return nil, errors.New("range partition overlaps")
	}

	mutex, ok := ps.rangePartitionLocks[rp.PartID]
	if !ok {
		return nil, errors.New("ps has no lock on partID")
	}

	//stop incoming requests and make sure only one is calling "rp.Close, rp.Split"
	ps.Lock()
	if _, ok := ps.rangePartitions[req.Partid]; !ok {
		ps.Unlock()
		return nil, errors.New("no such partid")
	}
	delete(ps.rangePartitions, req.Partid)
	ps.Unlock()

	midKey := rp.GetSplitPoint()

	if len(midKey) == 0 {
		return nil, errors.New("mid key is zero")
	}

	//如果submitGC/submitCompact在Close之前, Close等待完成
	//如果submitGC/submitCompact在Close之后, 由于Close设置了writeBlock, submitGC/submitCompact直接返回
	rp.Close()

	fmt.Printf("rp %d is closed", rp.PartID)
	xlog.Logger.Infof("rp %d is closed", rp.PartID)

	//LogRowStreamEnd MUST be called after rp.Close()
	//sm service use logEnd and rowEnd to seal log stream and row stream
	ends := rp.LogRowMetaStreamEnd()
	rp = nil

	backOffTime := time.Second
	for {
		err := ps.smClient.MultiModifySplit(ctx, req.Partid, midKey, mutex.Key(), mutex.Header().Revision,
			ends.LogEnd, ends.RowEnd, ends.MetaEnd)

		xlog.Logger.Infof("range partionion %d split: result is %v", err)
		if err == nil {
			break
		} else {
			xlog.Logger.Errorf("range partionion %d split: result is %v, retry...", err)
			time.Sleep(backOffTime)
			backOffTime *= 2
			continue
		}
	}

	defer func() {
		//release distributed lock
		ps.Lock()
		delete(ps.rangePartitionLocks, req.Partid)
		ps.Unlock()
		mutex.Unlock(context.Background())
	}()

	return &pspb.SplitPartResponse{}, nil
}
