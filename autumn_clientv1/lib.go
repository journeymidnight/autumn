package autumn_clientv1

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type AutumnLib struct {
	etcdClient      *clientv3.Client
	etcdAddr        []string
	regions         []*pspb.RegionInfo
	psDetails       map[uint64]*pspb.PSDetail
	utils.SafeMutex //protect regions and psDetails
	closeWatch      func()

	conns    map[string]*grpc.ClientConn
	connLock utils.SafeMutex //protect conns
}

func NewAutumnLib(etcdAddr []string) *AutumnLib {
	return &AutumnLib{
		etcdAddr:  etcdAddr,
		psDetails: make(map[uint64]*pspb.PSDetail),
		conns:     make(map[string]*grpc.ClientConn),
	}
}

func (lib *AutumnLib) Close() {
	lib.etcdClient.Close()
}

func (lib *AutumnLib) saveRegion(regions *pspb.Regions) {

	newRegions := make([]*pspb.RegionInfo, len(regions.Regions))
	i := 0
	for _, region := range regions.Regions {
		newRegions[i] = region
		i++
	}
	sort.Slice(newRegions, func(i, j int) bool {
		return bytes.Compare(newRegions[i].Rg.StartKey, newRegions[j].Rg.StartKey) < 0
	})
	lib.Lock()
	lib.regions = newRegions
	lib.Unlock()
}

func (lib *AutumnLib) Connect() error {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   lib.etcdAddr,
		DialTimeout: time.Second,
	})
	lib.etcdClient = client

	var maxRev int64
	data, rev, err := etcd_utils.EtcdGetKV(client, "regions/config")
	if err == nil {
		var regions pspb.Regions
		utils.MustUnMarshal(data, &regions)
		//sort and save regions
		lib.saveRegion(&regions)
	}
	maxRev = utils.Max64(maxRev, rev)

	var kvs []*mvccpb.KeyValue
	kvs, rev, err = etcd_utils.EtcdRange(client, "PSSERVER/")
	if err == nil {
		for _, kv := range kvs {
			var ps pspb.PSDetail
			utils.MustUnMarshal(kv.Value, &ps)
			lib.psDetails[ps.PSID] = &ps
		}
	}
	maxRev = utils.Max64(maxRev, rev)

	watch1, close1 := etcd_utils.EtcdWatchEvents(client, "regions/config", "", maxRev)
	go func() {
		for res := range watch1 {
			//skip to the last, only cares about latest config
			fmt.Printf("%+v\n", res)
			e := res.Events[len(res.Events)-1]
			var regions pspb.Regions
			if err = regions.Unmarshal(e.Kv.Value); err != nil {
				xlog.Logger.Errorf(err.Error())
				continue
			}
			lib.saveRegion(&regions)
		}
	}()

	watch2, close2 := etcd_utils.EtcdWatchEvents(client, "PSSERVER/", "PSSERVER0", maxRev)
	go func() {
		for res := range watch2 {
			for _, e := range res.Events {
				var psDetail pspb.PSDetail
				switch e.Type.String() {
				case "PUT":
					if err = psDetail.Unmarshal(e.Kv.Value); err != nil {
						break
					}
					lib.Lock()
					lib.psDetails[psDetail.PSID] = &psDetail
					lib.Unlock()
				case "DELETE":
					if err = psDetail.Unmarshal(e.PrevKv.Value); err != nil {
						break
					}
					lib.Lock()
					delete(lib.psDetails, psDetail.PSID)
					lib.Unlock()
				}
			}
		}
	}()

	lib.closeWatch = func() {
		close1()
		close2()
	}
	return nil
}

//会不会有可能PS更新的慢, 没有得到最新的PS,导致psDetails里面是空,
//这里就loop ever
func (lib *AutumnLib) getPSAddr(psID uint64) string {
	for {
		lib.RLock()
		detail := lib.psDetails[psID]
		lib.RUnlock()
		if detail != nil {
			return detail.Address
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (lib *AutumnLib) getConn(addr string) *grpc.ClientConn {
	lib.connLock.RLock()
	conn, ok := lib.conns[addr]
	lib.connLock.RUnlock()
	if ok {
		return conn
	}

	var err error
	for {
		conn, err = grpc.Dial(addr,
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(33<<20),
				grpc.MaxCallSendMsgSize(33<<20)),
			grpc.WithBackoffMaxDelay(time.Second),
			grpc.WithInsecure())
		if err == nil {
			break
		}
		xlog.Logger.Error(err)
		time.Sleep(5 * time.Millisecond)
	}
	lib.connLock.Lock()
	lib.conns[addr] = conn
	lib.connLock.Unlock()

	return conn
}

func (lib *AutumnLib) getRegions() []*pspb.RegionInfo {
	lib.RLock()
	defer lib.RUnlock()
	return lib.regions
}

func (lib *AutumnLib) Put(ctx context.Context, key, value []byte) error {
	if len(key) == 0 || len(value) == 0 {
		return errors.New("key or value is empty")
	}
	if len(value) > 32<<20 {
		return errors.New("value is too large")
	}
	sortedRegions := lib.getRegions()
	if len(sortedRegions) == 0 {
		return errors.New("no regions to write")
	}
	idx := sort.Search(len(sortedRegions), func(i int) bool {
		if len(sortedRegions[i].Rg.EndKey) == 0 {
			return true
		}
		return bytes.Compare(sortedRegions[i].Rg.EndKey, key) > 0
	})

	conn := lib.getConn(lib.getPSAddr((sortedRegions[idx].PSID)))
	client := pspb.NewPartitionKVClient(conn)
	_, err := client.Put(ctx, &pspb.PutRequest{
		Key:    key,
		Value:  value,
		Partid: sortedRegions[idx].PartID,
	})
	return err
}

func (lib *AutumnLib) Get(ctx context.Context, key []byte) ([]byte, error) {
	sortedRegions := lib.getRegions()
	if len(sortedRegions) == 0 {
		return nil, errors.New("no regions to write")
	}
	//idx
	idx := sort.Search(len(sortedRegions), func(i int) bool {
		if len(sortedRegions[i].Rg.EndKey) == 0 {
			return true
		}
		return bytes.Compare(sortedRegions[i].Rg.EndKey, key) > 0
	})

	conn := lib.getConn(lib.getPSAddr((sortedRegions[idx].PSID)))
	client := pspb.NewPartitionKVClient(conn)
	res, err := client.Get(ctx, &pspb.GetRequest{
		Key:    key,
		Partid: sortedRegions[idx].PartID,
	})

	if err != nil {
		return nil, err
	}
	return res.Value, err

}

func (lib *AutumnLib) Range(ctx context.Context, prefix []byte, start []byte, limit uint32) ([][]byte, bool, error) {
	sortedRegions := lib.getRegions()
	if len(sortedRegions) == 0 {
		return nil, false, errors.New("no regions to write")
	}
	//FIXME: 多range partition的情况
	//idx
	idx := sort.Search(len(sortedRegions), func(i int) bool {
		if len(sortedRegions[i].Rg.EndKey) == 0 {
			return true
		}
		return bytes.Compare(sortedRegions[i].Rg.EndKey, start) > 0
	})
	//start from idx
	results := make([][]byte, 0)
	var more bool
	for i := idx; i < len(sortedRegions) && limit > 0; i++ {
		if i != idx {
			if !bytes.HasPrefix(sortedRegions[i].Rg.StartKey, prefix) {
				break
			}
		}
		//fmt.Printf("range from partID %d, startKey %s \n", sortedRegions[i].PartID, string(sortedRegions[i].Rg.StartKey))
		conn := lib.getConn(lib.getPSAddr((sortedRegions[i].PSID)))
		client := pspb.NewPartitionKVClient(conn)
		res, err := client.Range(ctx, &pspb.RangeRequest{
			Prefix: prefix,
			Start:  start,
			Limit:  limit,
			Partid: sortedRegions[i].PartID,
		})
		if err != nil {
			return nil, false, err
		}

		limit -= uint32(len(res.Keys))
		more = (res.Truncated == 1)
		//print len of res.Keys
		fmt.Printf("i :%d, len of res.Keys %d\n", i, len(res.Keys))
		results = append(results, res.Keys...)

	}

	return results, more, nil
}

func (lib *AutumnLib) SplitPart(ctx context.Context, partID uint64) error {
	sortedRegions := lib.getRegions()
	foundRegion := -1
	for i := 0; i < len(sortedRegions); i++ {
		if sortedRegions[i].PartID == partID {
			foundRegion = i
		}
	}
	if foundRegion == -1 {
		return errors.New("partition not found")
	}

	conn := lib.getConn(lib.getPSAddr(sortedRegions[foundRegion].PSID))
	client := pspb.NewPartitionKVClient(conn)
	_, err := client.SplitPart(ctx, &pspb.SplitPartRequest{
		PartID: partID,
	})

	return err
}

func (lib *AutumnLib) Delete(ctx context.Context, key []byte) error {
	sortedRegions := lib.getRegions()
	if len(sortedRegions) == 0 {
		return errors.New("no regions to write")
	}
	//idx
	idx := sort.Search(len(sortedRegions), func(i int) bool {
		if len(sortedRegions[i].Rg.EndKey) == 0 {
			return true
		}
		return bytes.Compare(sortedRegions[i].Rg.EndKey, key) > 0
	})

	conn := lib.getConn(lib.getPSAddr((sortedRegions[idx].PSID)))
	client := pspb.NewPartitionKVClient(conn)
	_, err := client.Delete(ctx, &pspb.DeleteRequest{
		Key:    key,
		Partid: sortedRegions[idx].PartID,
	})

	if err != nil {
		return err
	}
	return err

}
