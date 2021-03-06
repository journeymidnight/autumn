package etcd_utils

import (
	"fmt"
	"net/url"
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

func mustParseURL(s string) []url.URL {
	u, _ := url.Parse(s)
	return []url.URL{*u}
}
func (suite *EtcdUtilTestSuite) SetupSuite() {
	xlog.InitLog([]string{"etcd.log"}, zapcore.DebugLevel)

	config := embed.NewConfig()
	config.Name = "etcd"
	config.Dir = "etcd.db"

		config.LCUrls = mustParseURL("http://127.0.0.1:2379")
		config.LPUrls = mustParseURL("http://127.0.0.1:2380")
		config.ACUrls = mustParseURL("http://127.0.0.1:2379")
		config.APUrls = mustParseURL("http://127.0.0.1:2380")
		config.InitialCluster = "etcd=http://127.0.0.1:2380"
		config.ClusterState = "new"
		config.InitialClusterToken ="cluster"
	
	etcd, client, err := ServeETCD(config)
	fmt.Println(err)
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
	data, _, err := EtcdGetKV(suite.client, "key_50")
	suite.Nil(err)
	suite.Equal("val50", string(data))
	
}

func (suite *EtcdUtilTestSuite) TestWatch() {
	go func(){
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key/%d", i)
			val := fmt.Sprintf("val%d", i)
			EtcdSetKV(suite.client, key, []byte(val))
		}
		data,_, err := EtcdGetKV(suite.client, "key/50")
		suite.Nil(err)
		suite.Equal("val50", string(data))
	}()

	ch, closeWatch := EtcdWatchEvents(suite.client, "key", "key0", 1)
	j := 0 
	for e := range ch {
		for i := range e.Events {
			suite.Equal(fmt.Sprintf("key/%d", j), string(e.Events[i].Kv.Key))
			j ++
		}
		if j == 99 {
			break
		}
	}
	closeWatch()

}

func TestEtcdUtil(t *testing.T) {
	suite.Run(t, new(EtcdUtilTestSuite))
}
