package partition_server

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/journeymidnight/autumn/manager/smclient"
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
	if bytes.Compare(rp.StartKey, key) <= 0 && (len(rp.EndKey) == 0 || bytes.Compare(key, rp.EndKey) < 0) {
		return rp
	}
	fmt.Println("key is not in range")
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
	//version可有三种
	//0, 取当前seqnumber
	//math.MaxUint64, 取最新的
	//其他, 取指定的version
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
	ps.RLock()
	rp := ps.rangePartitions[req.Partid]
	ps.RUnlock()
	if rp == nil {
		return nil, errors.New("no such partid")
	}
	
	out := rp.Range(req.Prefix, req.Start, req.Limit)

	var truncated uint32
	if len(out) == int(req.Limit) {
		truncated = 1
	} else {
		truncated = 0
	}
	return &pspb.RangeResponse{
		Truncated: truncated,
		Keys:      out,
	}, nil
}

func (ps *PartitionServer) SplitPart(ctx context.Context, req *pspb.SplitPartRequest) (*pspb.SplitPartResponse, error) {
	ps.RLock()
	rp := ps.rangePartitions[req.PartID]
	ps.RUnlock()
	if rp == nil {
		fmt.Printf("no such rp %d\n", req.PartID)
		return nil, errors.New("no such partid")
	}
	
	if !rp.CanSplit() {
		return nil, errors.New("range partition overlaps")
	}
	

	mutex, ok := ps.rangePartitionLocks[rp.PartID]
	if !ok {
		return nil, errors.New("ps has no lock on partID")
	}
	defer func() {
		ps.Lock()
		delete(ps.rangePartitionLocks, rp.PartID)
		ps.Unlock()
		mutex.Unlock(context.Background())
	}()


	//stop incoming requests and make sure only one is calling "rp.Close, rp.Split"
	ps.Lock()
	if _, ok := ps.rangePartitions[req.PartID] ; !ok {
		ps.Unlock()
		return nil, errors.New("no such partid")
	}
	delete(ps.rangePartitions, req.PartID)
	ps.Unlock()


	
	midKey := rp.GetSplitPoint()

	if len(midKey) == 0 {
		return nil, errors.New("mid key is zero")
	}
	

	rp.Close()

	fmt.Printf("rp %d is closed", rp.PartID)
	xlog.Logger.Infof("rp %d is closed", rp.PartID)
	
	
	//LogRowStreamEnd MUST called before rp.Close()
	//sm service use logEnd and rowEnd to seal log stream and row stream
	logEnd, rowEnd := rp.LogRowStreamEnd()

	backOffTime := time.Second
	for {
		err := ps.smClient.MultiModifySplit(ctx, rp.PartID, midKey, mutex.Key(), mutex.Header().Revision, logEnd, rowEnd)
		xlog.Logger.Infof("range partionion %d split: resut is %v", err)
		if err == nil {
			break
		} else if err == smclient.ErrTimeOut || err == wire_errors.NotLeader{
			time.Sleep(backOffTime)
			backOffTime *= 2
			continue
		} else {
			//I have to reopen rp
			//send warning to sm service
			defer panic("I have to reopen rp, panic myself to reopen all range partitions")
			return nil, err
		}
	}
	return nil, nil
}

