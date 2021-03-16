package manager

import (
	"fmt"
	"os"
	"testing"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
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

	config := &Config{
		Name:                "etcd",
		Dir:                 "etcd.db",
		ClientUrls:          "http://127.0.0.1:2379",
		PeerUrls:            "http://127.0.0.1:2380",
		AdvertiseClientUrls: "http://127.0.0.1:2379",
		AdvertisePeerUrls:   "http://127.0.0.1:2380",
		InitialCluster:      "etcd=http://127.0.0.1:2380",
		InitialClusterState: "new",
		ClusterToken:        "cluster",
		GrpcUrlSM:             "127.0.0.1:3000",
	}
	etcd, client, err := ServeETCD(config)
	suite.Nil(err)
	suite.etcd = etcd
	suite.client = client
}

func (suite *EtcdUtilTestSuite) TearDownSuite() {
	suite.etcd.Close()
	os.RemoveAll("etcd.db")
	os.Remove("etcd.log")
}

func (suite *EtcdUtilTestSuite) TestSetGetKV() {
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val%d", i)
		EtcdSetKV(suite.client, key, []byte(val))
	}
	data, err := EtcdGetKV(suite.client, "key_50")
	suite.Nil(err)
	suite.Equal("val50", string(data))
}

func TestEtcdUtil(t *testing.T) {
	suite.Run(t, new(EtcdUtilTestSuite))
}
