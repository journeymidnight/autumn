package main

import (
	"bytes"
	"context"
	"math"
	"sort"
	"time"

	"github.com/journeymidnight/autumn/manager/pmclient"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type AutumnLib struct {
	pm              *pmclient.AutumnPMClient
	pmAddr          []string
	regions         []*pspb.RegionInfo
	utils.SafeMutex //protect regions

	conns    map[string]*grpc.ClientConn
	connLock utils.SafeMutex //protect conns
}

func NewAutumnLib(pmAddr []string) *AutumnLib {
	return &AutumnLib{
		pmAddr: pmAddr,
		pm:     pmclient.NewAutumnPMClient(pmAddr),
		conns:  make(map[string]*grpc.ClientConn),
	}
}

func (lib *AutumnLib) Connect() error {
	if err := lib.pm.Connect(); err != nil {
		return err
	}
	lib.update()
	return nil
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
		conn, err = grpc.Dial(addr, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
		if err == nil {
			break
		}
		//FIXME: client log
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

func (lib *AutumnLib) update() {
	//FIXME: if psversion not match or reject by servers, update
	//loop forever
	var newRegions []*pspb.RegionInfo
	for {
		newRegions = lib.pm.GetRegions()
		if newRegions != nil {
			break
		}
		time.Sleep(time.Second)
	}
	//sort by StartKEY
	sort.Slice(newRegions, func(i, j int) bool {
		return bytes.Compare(newRegions[i].Rg.StartKey, newRegions[j].Rg.StartKey) < 0
	})

	lib.Lock()
	lib.regions = newRegions
	lib.Unlock()

}

func (lib *AutumnLib) Put(ctx context.Context, key, value []byte) error {
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

	conn := lib.getConn(sortedRegions[idx].Addr)
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

	conn := lib.getConn(sortedRegions[idx].Addr)
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

func (lib *AutumnLib) Range(ctx context.Context, prefix []byte, start []byte) ([][]byte, error) {
	sortedRegions := lib.getRegions()
	if len(sortedRegions) == 0 {
		return nil, errors.New("no regions to write")
	}
	//FIXME: 多range partition的情况
	//idx
	idx := sort.Search(len(sortedRegions), func(i int) bool {
		if len(sortedRegions[i].Rg.EndKey) == 0 {
			return true
		}
		return bytes.Compare(sortedRegions[i].Rg.EndKey, prefix) > 0
	})
	conn := lib.getConn(sortedRegions[idx].Addr)
	client := pspb.NewPartitionKVClient(conn)
	res, err := client.Range(ctx, &pspb.RangeRequest{
		Prefix: prefix,
		Start:  start,
		Limit:  math.MaxUint32,
		Partid: sortedRegions[idx].PartID,
	})
	if err != nil {
		return nil, err
	}
	return res.Keys, nil
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

	conn := lib.getConn(sortedRegions[idx].Addr)
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
