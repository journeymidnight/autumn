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

	"go.uber.org/zap/zapcore"
)

var (
	_ = fmt.Println
)

func init() {
	xlog.InitLog([]string{"test.log"}, zapcore.DebugLevel)
}

type ExtentNodeTestSuite struct {
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

func setupStreamManager(suite *ExtentNodeTestSuite, dir string) {
	var config = &manager.Config{
		Name:                "sm1",
		Dir:                 fmt.Sprintf("%s/sm1.db", dir),
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
	time.Sleep(17 * time.Second) //wait to be leader
}

func (suite *ExtentNodeTestSuite) SetupSuite() {

	//start sm
	//tmpdir := "node_test"
	tmpdir, err := ioutil.TempDir(os.TempDir(), "node_test")
	if err != nil {
		panic(err)
	}
	setupStreamManager(suite, tmpdir)

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
	for i := 0; i < 4; i++ {
		dir := fmt.Sprintf("%s/store%d", tmpdir, i)
		os.Mkdir(dir, 0777)

		_, err := FormatDisk(dir)
		if err != nil {
			panic(err)
		}
		url := fmt.Sprintf("127.0.0.1:400%d", i)

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
		url := fmt.Sprintf("127.0.0.1:400%d", i)
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

	fmt.Printf("NodeIDs is %v\n", nodeIDs)

	suite.tmpdir = tmpdir

}

func (suite *ExtentNodeTestSuite) TearDownSuite() {
	suite.sm.Close()
	suite.etcd.Close()
	os.RemoveAll(suite.tmpdir)

}

func (suite *ExtentNodeTestSuite) TestAppendReadValue() {
	sm := smclient.NewSMClient([]string{"127.0.0.1:3401"})
	err := sm.Connect()
	suite.Require().Nil(err)

	si, _, err := sm.CreateStream(context.Background(), 2, 1)
	suite.Require().Nil(err)
	//fmt.Printf("%v", ei)

	em := smclient.NewExtentManager(sm, []string{"127.0.0.1:2379"}, func(eventType string, cur *pb.ExtentInfo, prev *pb.ExtentInfo) {
		//fmt.Printf("updates: %s: %+v from %+v\n", eventType, cur, prev)
	})

	suite.mutex.Lock(context.Background())
	defer suite.mutex.Unlock(context.Background())
	sc := streamclient.NewStreamClient(sm, em, si.StreamID, streamclient.MutexToLock(suite.mutex))
	err = sc.Connect()
	suite.Require().Nil(err)
	extentID, offsets, end, err := sc.Append(context.Background(),
		[]*pb.Block{
			{Data:[]byte("hello")},
			{Data:[]byte("world")},
		}, true)
	suite.Require().Nil(err)
	suite.Require().True(len(offsets) > 0)
	//fmt.Printf("%d=>%d, on extent %d\n", offsets[0], end, extentID)

	blockReader := streamclient.NewAutumnBlockReader(em, sm)
	ret, end, err := blockReader.Read(context.Background(), extentID, offsets[0], 2, streamclient.HintReadFromCache)
	suite.Require().Nil(err)

	fmt.Printf("ret:%v, end %d\n", ret, end)
	suite.Require().Equal([]byte("hello"), ret[0].Data)
	suite.Require().Equal([]byte("world"), ret[1].Data)

}

func (suite *ExtentNodeTestSuite) TestNodeRecoveryDataFromOtherNode() {
	sm := smclient.NewSMClient([]string{"127.0.0.1:3401"})
	err := sm.Connect()
	suite.Require().Nil(err)

	si, _, err := sm.CreateStream(context.Background(), 2, 1)
	suite.Require().Nil(err)

	em := smclient.NewExtentManager(sm, []string{"127.0.0.1:2379"}, func(eventType string, cur *pb.ExtentInfo, prev *pb.ExtentInfo) {
		//fmt.Printf("%s: %+v from %+v", eventType, cur, prev)
	})

	suite.mutex.Lock(context.Background())
	defer suite.mutex.Unlock(context.Background())

	sc := streamclient.NewStreamClient(sm, em, si.StreamID, streamclient.MutexToLock(suite.mutex))
	err = sc.Connect()
	suite.Require().Nil(err)
	extentID, offsets, _, err := sc.Append(context.Background(),
		[]*pb.Block{
			{Data:[]byte("hello")},
			{Data:[]byte("world")},
		}, true)
	suite.Require().Nil(err)
	suite.Require().True(len(offsets) > 0)

	err = sc.MustAllocNewExtent()
	suite.Nil(err)

	em.WaitVersion(extentID, 2)//wait for version 2
	

	//原来是(7,5,3), 改成(1,5,3)
	suite.Require().Equal(uint64(1), suite.ens[0].nodeID)
	//发请求到node 1
	res, err := suite.ens[0].RequireRecovery(context.Background(), &pb.RequireRecoveryRequest{
		Task: &pb.RecoveryTask{
			ExtentID:  extentID,
			ReplaceID: 7,
			NodeID: 1,
		},
	})
	fmt.Printf("%+v\n", res)
	time.Sleep(3 * time.Second)

	eod := suite.ens[0].getExtent(extentID)//恢复后的extent
	suite.NotNil(eod)
	
}

func TestNode(t *testing.T) {
	suite.Run(t, new(ExtentNodeTestSuite))
}
