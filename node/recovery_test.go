package node

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/manager"
	smclient "github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/manager/stream_manager"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/embed"

	"google.golang.org/grpc"
)

var (
	_ = fmt.Println
)

//FIXME:: deduplicate test code
type RcoveryTestSuite struct {
	suite.Suite
	ens      [4]*ExtentNode
	tmpdir   string
	sm       *stream_manager.StreamManager
	smServer *grpc.Server
	etcd     *embed.Etcd
	mutex    *concurrency.Mutex
	session  *concurrency.Session
	client   *clientv3.Client
}

func setupStreamManager1(suite *RcoveryTestSuite, dir string) {
	var config = &manager.Config{
		Name:                "sm1",
		Dir:                 fmt.Sprintf("%s/sm.db", dir),
		ClientUrls:          "http://127.0.0.1:2379",
		PeerUrls:            "http://127.0.0.1:12380",
		AdvertisePeerUrls:   "http://127.0.0.1:12380",
		AdvertiseClientUrls: "http://127.0.0.1:2379",
		InitialCluster:      "sm1=http://127.0.0.1:12380",
		InitialClusterState: "new",
		ClusterToken:        "sm-cluster-1",
		GrpcUrl:             "127.0.0.1:3401",
	}

	cfg, err := config.GetEmbedConfig()
	if err != nil {
		fmt.Println(err)
		xlog.Logger.Fatal(err)
	}

	etcd, client, err := etcd_utils.ServeETCD(cfg)

	if err != nil {
		panic(err.Error())
	}
	sm := stream_manager.NewStreamManager(etcd, client, config)
	go sm.LeaderLoop()

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(64<<20),
		grpc.MaxSendMsgSize(64<<20),
		grpc.MaxConcurrentStreams(1000),
	)
	sm.RegisterGRPC(grpcServer)
	listener, err := net.Listen("tcp", config.GrpcUrl)
	if err != nil {
		xlog.Logger.Fatalf(err.Error())
	}
	go func() {
		err = grpcServer.Serve(listener)
		if err != nil {
			xlog.Logger.Fatalf(err.Error())
		}
	}()

	suite.sm = sm
	suite.smServer = grpcServer
	suite.etcd = etcd
	suite.client = client
	time.Sleep(3 * time.Second) //wait to be leader
}

func (suite *RcoveryTestSuite) SetupSuite() {

	//start sm
	//tmpdir := "node_test"
	tmpdir, err := ioutil.TempDir(os.TempDir(), "node_test")
	if err != nil {
		panic(err)
	}
	setupStreamManager1(suite, tmpdir)

	smc := smclient.NewSMClient([]string{"127.0.0.1:3401"})
	err = smc.Connect()
	if err != nil {
		panic(err)
	}

	session, err := concurrency.NewSession(suite.client, concurrency.WithTTL(30))
	if err != nil {
		panic(err.Error())
	}
	suite.session = session

	suite.mutex = concurrency.NewMutex(session, "lockCouldHaveAnyName")

	var nodeIDs []uint64
	//format disk, generate dirs
	for i := 0; i < len(suite.ens); i++ {
		dir := fmt.Sprintf("%s/store%d", tmpdir, i)
		os.Mkdir(dir, 0777)

		_, err := FormatDisk(dir)
		if err != nil {
			panic(err)
		}
		//different with node_test.go's URL, make sure this next URL bind is always quick and successfully
		url := fmt.Sprintf("127.0.0.1:410%d", i)

		nodeID, uuidToDiskID, err := smc.RegisterNode(context.Background(), []string{fmt.Sprintf("uuid%d", i)}, url)
		err = ioutil.WriteFile(dir+"/node_id", []byte(fmt.Sprintf("%d", nodeID)), 0644)
		if err != nil {
			panic(err)
		}
		nodeIDs = append(nodeIDs, nodeID)
		err = ioutil.WriteFile(dir+"/disk_id", []byte(fmt.Sprintf("%d", uuidToDiskID[fmt.Sprintf("uuid%d", i)])), 0644)
		if err != nil {
			panic(err)
		}
	}

	for i := range suite.ens {
		dir := fmt.Sprintf("%s/store%d", tmpdir, i)
		url := fmt.Sprintf("127.0.0.1:410%d", i)
		//register to stream manager

		suite.ens[i] = NewExtentNode(nodeIDs[i], []string{dir}, "", url, []string{"127.0.0.1:3401"}, []string{"127.0.0.1:2379"})
		err := suite.ens[i].LoadExtents()
		if err != nil {
			panic(err)
		}
		err = suite.ens[i].ServeGRPC()
		if err != nil {
			panic(err)
		}
	}

	suite.tmpdir = tmpdir

}

func (suite *RcoveryTestSuite) TestAutoRecovery() {
	sm := smclient.NewSMClient([]string{"127.0.0.1:3401"})
	err := sm.Connect()
	suite.Require().Nil(err)

	//make sure CreateStream is successful
	si, _, err := sm.CreateStream(context.Background(), 3, 0)

	suite.Require().Nil(err)

	em := smclient.NewExtentManager(sm, []string{"127.0.0.1:2379"}, func(eventType string, cur *pb.ExtentInfo, prev *pb.ExtentInfo) {
		//fmt.Printf("updates: %s: %+v from %+v\n", eventType, cur, prev)
	})

	err = suite.mutex.Lock(context.Background())
	suite.Require().Nil(err)
	defer suite.mutex.Unlock(context.Background())

	sc := streamclient.NewStreamClient(sm, em, testExtentSize, si.StreamID, streamclient.MutexToLock(suite.mutex))
	err = sc.Connect()
	suite.Require().Nil(err)
	extentID, _, _, err := sc.Append(context.Background(),
		[][]byte{
			[]byte("hello"),
			[]byte("world"),
		}, false)
	suite.Require().Nil(err)
	//find where extentID is located
	exInfo, err := sm.ExtentInfo(context.Background(), extentID)
	suite.Require().Nil(err)
	suite.Require().Equal(3, len(exInfo.Replicates))
	//extentID is on 3 nodes, choose the first one to close
	nodeID := exInfo.Replicates[0]

	fmt.Printf("shutdown nodeID %d\n", nodeID)
	//shutdown node
	for i := range suite.ens {
		if suite.ens[i].nodeID == nodeID {
			suite.ens[i].Shutdown()
			break
		}
	}

	newExID, _, _, err := sc.Append(context.Background(),
		[][]byte{
			[]byte("autoretry"),
		}, false)

	suite.Require().Nil(err)
	fmt.Printf("stream extents from [%d] => [%d, %d]\n", extentID, extentID, newExID)
	suite.Require().NotEqual(extentID, newExID)
}

func (suite *RcoveryTestSuite) TearDownSuite() {
	suite.session.Close()
	for _, en := range suite.ens {
		en.Shutdown()
	}
	suite.sm.Close()
	suite.etcd.Close()

	os.RemoveAll(suite.tmpdir)
}

func TestAutoRecovery(t *testing.T) {
	suite.Run(t, new(RcoveryTestSuite))
}
