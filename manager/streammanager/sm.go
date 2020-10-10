package sm

import (
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/journeymidnight/autumn/manager"
)

type StreamManager struct {
	streams    *sync.Map //streamID=>pb.OrderedExtentIDs
	replicates *sync.Map //extentID=>pb.OrderedNodesReplicates
	nodes      *sync.Map //"nodeid" => "addr" //support update IP address
}

func NewStreamManager(etcd *embed.Etcd, client *clientv3.Client, config *manager.Config) {

}

func (sm *StreamManager) ServeGRPC() error {
	return nil
}

func (sm *StreamManager) Close() {

}
