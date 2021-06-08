package pmclient

import (
	"time"

	"github.com/journeymidnight/autumn/proto/pspb"
	"go.etcd.io/etcd/clientv3"
)



type AutumnPMClient struct {
	addrs      []string
	client     *clientv3.Client
}

//connect to etcd address
func NewAutumnPMClient(addrs []string) *AutumnPMClient {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   addrs,
		DialTimeout: time.Second,
	})
	if err != nil {
		return nil
	}

	return &AutumnPMClient{
		addrs: addrs,
		client: client,
	}

}

func (client *AutumnPMClient) SetRowStreamTables(id uint64, tables []*pspb.Location) error {

}


func (client *AutumnPMClient) Bootstrap(logID uint64, rowID uint64, psID uint64) (uint64, error) {
}

func (client *AutumnPMClient) GetPSInfo() (ret []*pspb.PSDetail) {

}