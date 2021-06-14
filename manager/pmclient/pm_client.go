package pmclient

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)
type AutumnPMClient struct {
	conns []*grpc.ClientConn //protected by RWMUTEX
	utils.SafeMutex
	lastLeader int32 //protected by atomic
	addrs      []string
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

func (client *AutumnPMClient) Bootstrap(logID uint64, rowID uint64) (uint64, error) {
	err := errors.New("unknow err")
	var partID uint64

	req := &pspb.BootstrapRequest{
		LogID:  logID,
		RowID:  rowID,
	}
	var res *pspb.BootstrapResponse

	client.try(func(conn *grpc.ClientConn) bool {
		c := pspb.NewPartitionManagerServiceClient(conn)
		res, err = c.Bootstrap(context.Background(), req)
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return true
		}
		if res.Code != pb.Code_OK {
			err = wire_errors.FromPBCode(res.Code, res.CodeDes)
			//if remote is not a leader, retry
			if err == wire_errors.NotLeader {
				return true
			}
			return false
		}
		partID = res.PartID
		return false

	}, 10*time.Millisecond)

	return partID, err
}
