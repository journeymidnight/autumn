package etcd_utils

import (
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/journeymidnight/autumn/xlog"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap/zapcore"
)

type EtcdUtilTestSuite struct {
	suite.Suite
	etcd   *embed.Etcd
	client *clientv3.Client
}

func mustParseURL(s string) []url.URL {
	u, _ := url.Parse(s)
	return []url.URL{*u}
}
func (suite *EtcdUtilTestSuite) SetupSuite() {
	xlog.InitLog([]string{"etcd.log"}, zapcore.DebugLevel)

	config := embed.NewConfig()
	config.Name = "etcd"
	config.Dir = "etcd.db"

	//prevent etcd port conflict with node_test.go
	config.LCUrls = mustParseURL("http://127.0.0.1:12379")
	config.LPUrls = mustParseURL("http://127.0.0.1:2380")
	config.ACUrls = mustParseURL("http://127.0.0.1:12379")
	config.APUrls = mustParseURL("http://127.0.0.1:2380")
	config.InitialCluster = "etcd=http://127.0.0.1:2380"
	config.ClusterState = "new"
	config.InitialClusterToken = "cluster"
	config.LogLevel = "fatal"
	etcd, client, err := ServeETCD(config)
	suite.Nil(err)
	suite.etcd = etcd
	suite.client = client
}

func (suite *EtcdUtilTestSuite) TearDownSuite() {
	suite.client.Close()
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
	data, _, err := EtcdGetKV(suite.client, "key_50")
	suite.Nil(err)
	suite.Equal("val50", string(data))

}

func (suite *EtcdUtilTestSuite) TestWatch() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key/%d", i)
			val := fmt.Sprintf("val%d", i)
			EtcdSetKV(suite.client, key, []byte(val))
		}

		data, _, err := EtcdGetKV(suite.client, "key/50")
		suite.Nil(err)
		suite.Equal("val50", string(data))
		wg.Done()

	}()

	ch, closeWatch := EtcdWatchEvents(suite.client, "key", "key0", 1)
	j := 0
	for e := range ch {
		for i := range e.Events {
			suite.Equal(fmt.Sprintf("key/%d", j), string(e.Events[i].Kv.Key))
			j++
		}
		if j == 99 {
			break
		}
	}
	closeWatch()
	wg.Wait()
}

func TestEtcdUtil(t *testing.T) {
	suite.Run(t, new(EtcdUtilTestSuite))
}
