package partitionmanager

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"

	"go.etcd.io/etcd/clientv3/concurrency"
	"google.golang.org/grpc"
)

const (
	idKey               = "AutumnPMIDKey"
	pmElectionKeyPrefix = "AutumnPMLeader"
)

/*
type PSStatus struct {
	pspb.PSDetail
	lastEcho time.Time
}
*/

type PartitionManager struct {
	etcd       *embed.Etcd
	client     *clientv3.Client
	grcpServer *grpc.Server
	ID         uint64 //backend ETCD server's ID

	isLeader    int32
	memberValue string
	//leadeKey is to store Election key
	leaderKey string
	config    *manager.Config

	pslock  utils.SafeMutex           //protect
	psNodes map[uint64]*pspb.PSDetail //cache from etcd, read lasted PSNodes when became leader

	partLock utils.SafeMutex
	partMeta map[uint64]*pspb.PartitionMeta

	allocIdLock utils.SafeMutex
}

func NewPartitionManager(etcd *embed.Etcd, client *clientv3.Client, config *manager.Config) *PartitionManager {
	pm := &PartitionManager{
		etcd:   etcd,
		client: client,
		config: config,
		ID:     uint64(etcd.Server.ID()),
	}

	v := pb.MemberValue{
		ID:      pm.ID,
		Name:    etcd.Config().Name + "_PM",
		GrpcURL: config.GrpcUrl,
	}

	data, err := v.Marshal()
	utils.Check(err)

	pm.memberValue = string(data)

	return pm

}

func (pm *PartitionManager) AmLeader() bool {
	return atomic.LoadInt32(&pm.isLeader) == 1
}

var parseError = errors.New("parse error")

func parseParts(kvs []*mvccpb.KeyValue) map[uint64]*pspb.PartitionMeta {
	parse := func(s string) (uint64, string, error) {
		parts := strings.Split(s, "/")
		if len(parts) != 3 {
			return 0, "", parseError
		}
		id, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, "", err
		}

		return id, parts[2], nil
	}

	var lastPartID uint64 = 0
	ret := make(map[uint64]*pspb.PartitionMeta)

	//依赖必须PARTID永远不为0, 需要在allocUniqID的时候,从1开始
	for _, kv := range kvs {
		fmt.Printf("reading %s\n", kv.Key)
		partID, suffix, err := parse(string(kv.Key))
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			continue
		}
		if partID != lastPartID {
			ret[partID] = &pspb.PartitionMeta{
				PartID: partID,
			}
		}
		lastPartID = partID

		switch suffix {
		case "blobStreams":
			var blobs pspb.BlobStreams
			if err = blobs.Unmarshal(kv.Value); err != nil {
				xlog.Logger.Errorf(err.Error())
				continue
			}
			ret[partID].Blobs = &blobs
		case "logStream":
			ret[partID].LogStream = binary.BigEndian.Uint64(kv.Value)
		case "rowStream":
			ret[partID].RowStream = binary.BigEndian.Uint64(kv.Value)
		case "tables":
			var tables pspb.TableLocations
			if err = tables.Unmarshal(kv.Value); err != nil {
				xlog.Logger.Warnf(err.Error())
				continue
			}
			ret[partID].Locs = &tables
		case "discard":
			ret[partID].Discard = kv.Value
		case "parent":
			ret[partID].Parent = binary.BigEndian.Uint64(kv.Value)
		case "range":
			var rg pspb.Range
			if err = rg.Unmarshal(kv.Value); err != nil {
				xlog.Logger.Errorf(err.Error())
				continue
			}
			ret[partID].Rg = &rg
		default:
			xlog.Logger.Errorf("unknow suffix: %s", suffix)
			continue
		}
	}

	fmt.Printf("ret is %+v\n\n", ret)

	return ret
}

func (pm *PartitionManager) runAsLeader() {
	pm.pslock.Lock()
	defer pm.pslock.Unlock()

	pm.partLock.Lock()
	defer pm.partLock.Unlock()
	//load data

	kvs, err := manager.EtcdRange(pm.client, "PSSERVER")
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return
	}
	pm.psNodes = make(map[uint64]*pspb.PSDetail)
	for _, kv := range kvs {
		psid, err := parseKey(string(kv.Key), "PSSERVER")
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		var detail pspb.PSDetail
		if err = detail.Unmarshal(kv.Value); err != nil {
			xlog.Logger.Warnf(err.Error())
			return
		}
		pm.psNodes[psid] = &detail
	}

	kvs, err = manager.EtcdRange(pm.client, "PART")
	if err != nil {
		xlog.Logger.Warnf(err.Error())
		return
	}
	pm.partMeta = parseParts(kvs)

	fmt.Printf("????? %+v", pm.partMeta[3].Rg)

	atomic.StoreInt32(&pm.isLeader, 1)
}

func (pm *PartitionManager) RegisterGRPC(grpcServer *grpc.Server) {
	/*
		grpcServer := grpc.NewServer(
			grpc.MaxRecvMsgSize(8<<20),
			grpc.MaxSendMsgSize(8<<20),
			grpc.MaxConcurrentStreams(1000),
		)
	*/

	pspb.RegisterPartitionManagerServiceServer(grpcServer, pm)
	/*
		listener, err := net.Listen("tcp", pm.config.GrpcUrlPM)
		if err != nil {
			return err
		}
		go func() {
			grpcServer.Serve(listener)
		}()
	*/
	pm.grcpServer = grpcServer
}

func (pm *PartitionManager) etcdLeader() uint64 {
	return uint64(pm.etcd.Server.Leader())
}

//FIXME:put into etcd_op
func (pm *PartitionManager) LeaderLoop() {
	for {
		if pm.ID != pm.etcdLeader() {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		s, err := concurrency.NewSession(pm.client, concurrency.WithTTL(15))
		if err != nil {
			xlog.Logger.Warnf(err.Error())
			continue
		}
		//returns a new election on a given key prefix
		e := concurrency.NewElection(s, pmElectionKeyPrefix)
		ctx := context.TODO()

		if err = e.Campaign(ctx, pm.memberValue); err != nil {
			xlog.Logger.Warnf(err.Error())
			continue
		}
		pm.leaderKey = e.Key()
		xlog.Logger.Infof("elected %d as leader", pm.ID)
		pm.runAsLeader()

		select {
		case <-s.Done():
			s.Close()
			atomic.StoreInt32(&pm.isLeader, 0)
			xlog.Logger.Info("%d's leadershipt expire", pm.ID)
		}
	}
}

func (pm *PartitionManager) Close() {

}

//FIXME
func parseKey(s string, prefix string) (uint64, error) {

	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return 0, errors.Errorf("parse key[%s] failed :", s)
	}
	if parts[0] != prefix {
		return 0, errors.Errorf("parse key[%s] failed, parts[0] not match :", s)
	}
	return strconv.ParseUint(parts[1], 10, 64)
}
