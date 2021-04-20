package pmclient

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

//FIXME: delete PMCLient, add function to range_partition
type PMClient interface {
	SetRowStreamTables(uint64, []*pspb.Location) error
}

type AutumnPMClient struct {
	conns []*grpc.ClientConn //protected by RWMUTEX
	utils.SafeMutex
	lastLeader int32 //protected by atomic
	addrs      []string
	PS         map[uint64]*pspb.PSDetail
	PSLock     utils.SafeMutex
}

func NewAutumnPMClient(addrs []string) *AutumnPMClient {
	utils.AssertTrue(xlog.Logger != nil)
	return &AutumnPMClient{
		addrs: addrs,
	}
}

func (client *AutumnPMClient) Connect() error {

	if len(client.addrs) == 0 {
		return errors.Errorf("addr is nil")
	}
	client.Lock()
	client.conns = nil
	errCount := 0
	for _, addr := range client.addrs {
		c, err := grpc.Dial(addr, grpc.WithBackoffMaxDelay(time.Second), grpc.WithInsecure())
		if err != nil {
			errCount++
		}
		client.conns = append(client.conns, c)
	}
	client.Unlock()
	if errCount == len(client.addrs) {
		return errors.Errorf("all connection failed")
	}
	atomic.StoreInt32(&client.lastLeader, 0)
	return nil
}

func (client *AutumnPMClient) try(f func(conn *grpc.ClientConn) bool, x time.Duration) {
	client.RLock()
	connLen := len(client.conns)
	client.RUnlock()

	current := atomic.LoadInt32(&client.lastLeader)
	for loop := 0; loop < connLen*2; loop++ {
		client.RLock()
		if client.conns != nil && client.conns[current] != nil {
			//if f() return true, sleep and continue
			//if f() return false, return
			if f(client.conns[current]) == true {
				current = (current + 1) % int32(connLen)
				client.RUnlock()
				time.Sleep(x)
				continue
			} else {
				atomic.StoreInt32(&client.lastLeader, current)
				client.RUnlock()
				return
			}
		}
	}
}

func (client *AutumnPMClient) SetRowStreamTables(id uint64, tables []*pspb.Location) error {
	aerr := errors.New("fail")
	client.try(func(conn *grpc.ClientConn) bool {
		c := pspb.NewPartitionManagerServiceClient(conn)
		res, err := c.SetRowStreamTables(context.Background(), &pspb.SetRowStreamTablesRequest{
			PartitionID: id,
			Locs: &pspb.TableLocations{
				Locs: tables,
			},
		})
		if err != nil || res.Code != pb.Code_OK {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		aerr = err
		return false

	}, 10*time.Millisecond)

	return aerr
}

func (client *AutumnPMClient) GetPartitionMeta(psid uint64) (ret []*pspb.PartitionMeta) {
	client.try(func(conn *grpc.ClientConn) bool {
		c := pspb.NewPartitionManagerServiceClient(conn)
		res, err := c.GetPartitionMeta(context.Background(), &pspb.GetPartitionMetaRequest{
			PSID: psid,
		})
		if err != nil {
			xlog.Logger.Warnf("%s from %s", err.Error(), conn.Target())
			return true
		}
		if res.Code != pb.Code_OK {
			xlog.Logger.Warnf("not Code_OK, %s from %s", res.Code.String(), conn.Target())
			return true
		}

		ret = res.Meta
		return false

	}, 10*time.Millisecond)
	return ret
}

func (client *AutumnPMClient) Bootstrap(logID uint64, rowID uint64, psID uint64) (uint64, error) {
	acerr := errors.New("unknow err")
	var partID uint64

	req := &pspb.BootstrapRequest{
		LogID:  logID,
		RowID:  rowID,
		Parent: psID,
	}
	client.try(func(conn *grpc.ClientConn) bool {
		c := pspb.NewPartitionManagerServiceClient(conn)
		res, err := c.Bootstrap(context.Background(), req)
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		acerr = nil
		partID = res.PartID
		return false

	}, 10*time.Millisecond)

	return partID, acerr
}

func (client *AutumnPMClient) GetPSInfo() (ret []*pspb.PSDetail) {
	client.try(func(conn *grpc.ClientConn) bool {
		c := pspb.NewPartitionManagerServiceClient(conn)
		res, err := c.GetPSInfo(context.Background(), &pspb.GetPSInfoRequest{})
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		ret = res.Servers
		return false

	}, 10*time.Millisecond)
	return ret
}

func (client *AutumnPMClient) GetRegions() (ret []*pspb.RegionInfo) {
	
	client.try(func(conn *grpc.ClientConn) bool {
		c := pspb.NewPartitionManagerServiceClient(conn)
		res, err := c.GetRegions(context.Background(), &pspb.GetRegionsRequest{})
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		if res.Code != pb.Code_OK {
			xlog.Logger.Warnf("not Code_OK")
			return true
		}
		ret = res.Regions
		return false

	}, 10*time.Millisecond)
	return ret

}

func (client *AutumnPMClient) RegisterSelf(address string) (uint64, error) {
	aerr := errors.New("loop timeout")
	var id uint64
	client.try(func(conn *grpc.ClientConn) bool {
		c := pspb.NewPartitionManagerServiceClient(conn)
		var res *pspb.RegisterPSResponse
		res, err := c.RegisterPS(context.Background(), &pspb.RegisterPSRequest{Addr: address})
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		if res.Code != pb.Code_OK {
			xlog.Logger.Warnf(res.Code.String())
		}
		id = res.Id
		aerr = nil
		return false

	}, 10*time.Millisecond)

	return id, aerr

}
