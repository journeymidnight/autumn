package dlock

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap/zapcore"
)

type EtcdUtilTestSuite struct {
	suite.Suite
	etcd   *embed.Etcd
	client *clientv3.Client
}

func (suite *EtcdUtilTestSuite) SetupSuite() {
	xlog.InitLog([]string{"etcd.log"}, zapcore.DebugLevel)

	config := &manager.Config{
		Name:                "etcd",
		Dir:                 "etcd.db",
		ClientUrls:          "http://127.0.0.1:2379",
		PeerUrls:            "http://127.0.0.1:2380",
		AdvertiseClientUrls: "http://127.0.0.1:2379",
		AdvertisePeerUrls:   "http://127.0.0.1:2380",
		InitialCluster:      "etcd=http://127.0.0.1:2380",
		InitialClusterState: "new",
		ClusterToken:        "cluster",
		GrpcUrl:             "127.0.0.1:3000",
	}
	etcd, client, err := manager.ServeETCD(config)
	suite.Nil(err)
	suite.etcd = etcd
	suite.client = client
}

func (suite *EtcdUtilTestSuite) TearDownSuite() {
	suite.etcd.Close()
	os.RemoveAll("etcd.db")
	os.Remove("etcd.log")
}

func (suite *EtcdUtilTestSuite) TestLock() {
	client = suite.client
	l1 := NewDLock("/mylock")
	l2 := NewDLock("/mylock")

	err := l1.Lock(1 * time.Second)
	suite.Nil(err)
	
	err = l2.Lock(1 * time.Second)
	suite.Equal(context.DeadlineExceeded, err)
	

	l1.Close() //release lock


	err = l2.Lock(time.Second)
	suite.Nil(err)
	l2.Close()

}

func TestEtcdUtil(t *testing.T) {
	suite.Run(t, new(EtcdUtilTestSuite))
}
