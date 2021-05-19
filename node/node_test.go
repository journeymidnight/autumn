package node

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/embed"
	"github.com/journeymidnight/autumn/manager"
	smclient "github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/manager/stream_manager"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/stretchr/testify/suite"

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
	ens [3]*ExtentNode
	tmpdir string
	sm    *stream_manager.StreamManager
	smServer   *grpc.Server
	etcd  *embed.Etcd
}

func setupStreamManager(ent *ExtentNodeTestSuite, dir string) {
	var config  = &manager.Config{
		Name: "sm1",
		Dir : fmt.Sprintf("%s/sm1.db", dir),             
		ClientUrls: "http://127.0.0.1:2379",     
		PeerUrls:   "http://127.0.0.1:12380",   
		AdvertisePeerUrls:  "http://127.0.0.1:12380",
		AdvertiseClientUrls: "http://127.0.0.1:2379",
		InitialCluster:   "sm1=http://127.0.0.1:12380",
		InitialClusterState: "new",
		ClusterToken:     "sm-cluster-1",
		GrpcUrl: "127.0.0.1:3401",
	}
	xlog.InitLog([]string{"manager.log"}, zapcore.InfoLevel)

	etcd, client, err := manager.ServeETCD(config)
	if err != nil {
		panic(err.Error())
	}
	sm := stream_manager.NewStreamManager(etcd, client, config)
	go sm.LeaderLoop()

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(8<<20),
		grpc.MaxSendMsgSize(8<<20),
		grpc.MaxConcurrentStreams(1000),
	)
	sm.RegisterGRPC(grpcServer)
	listener, err := net.Listen("tcp", config.GrpcUrl)
	if err != nil {
		xlog.Logger.Fatalf(err.Error())
	}
	go func() {
		grpcServer.Serve(listener)
	}()

	ent.sm = sm
	ent.smServer = grpcServer
	ent.etcd = etcd
	time.Sleep(17 * time.Second) //wait to be leader

	smc := smclient.NewSMClient([]string{"127.0.0.1:3401"})
	err = smc.Connect()
	if err != nil {
		panic(err)
	}
	for i := 0 ;i < 3 ; i ++ {
		url := fmt.Sprintf("127.0.0.1:400%d", i+1)
		_, err := smc.RegisterNode(context.Background(), url)
		if err != nil {
			panic(err)
		}
	}



}

func (suite *ExtentNodeTestSuite) SetupSuite() {

	//start sm
	//tmpdir := "node_test"
	tmpdir , err := ioutil.TempDir(os.TempDir(), "node_test")
	if err != nil {
		panic(err)
	}
	setupStreamManager(suite, tmpdir)

	//start node
	for i := 1 ;i <= 3 ;i ++ {
		dir := fmt.Sprintf("%s/store%d", tmpdir, i)
		os.Mkdir(dir, 0777)

		err = FormatDisk(dir)
		if err != nil {
			panic(err)
		}
		err = ioutil.WriteFile(dir+"/node_id", []byte(fmt.Sprintf("%d", i)), 0644)
		if err != nil {
			panic(err)
		}
	}

	for i := range suite.ens {
		dir := fmt.Sprintf("%s/store%d", tmpdir, i+1)
		url := fmt.Sprintf("127.0.0.1:400%d", i+1)
		//register to stream manager

		suite.ens[i] = NewExtentNode(uint64(i+1), []string{dir}, "", url, []string{"127.0.0.1:3401"})
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

func (suite *ExtentNodeTestSuite) TearDownSuite() {
	suite.sm.Close()
	suite.etcd.Close()
	os.RemoveAll(suite.tmpdir)

}



func (suite *ExtentNodeTestSuite) TestAppendReadValue() {
	sm := smclient.NewSMClient([]string{"127.0.0.1:3401"})
	err := sm.Connect()
	suite.Require().Nil(err)

	si, ei, err := sm.CreateStream(context.Background(), 2, 1)
	suite.Require().Nil(err)
	fmt.Printf("%v", ei)
	

	em := smclient.NewExtentManager(sm)

	sc := streamclient.NewStreamClient(sm, em, si.StreamID)
	err = sc.Connect()
	suite.Require().Nil(err)
	extentID, offsets , _, err := sc.Append(context.Background(), 
				[]*pb.Block{
					{[]byte("hello")},
				    {[]byte("world")},
				})
	suite.Require().Nil(err)
	suite.Require().True(len(offsets)>0)
	//fmt.Printf("%d=>%d, on extent %d\n", offsets[0], end, extentID)
	
	blockReader := streamclient.NewAutumnBlockReader(em, sm)
	ret , err := blockReader.Read(context.Background(), extentID, offsets[0], 2)
	suite.Require().Nil(err)
	/*
	for i := range ret {
		fmt.Printf("%s\n", ret[i])
	}
	*/
	suite.Require().Equal([]byte("hello"), ret[0].Data)
	suite.Require().Equal([]byte("world"), ret[1].Data)

}


func TestNode(t *testing.T) {
	suite.Run(t, new(ExtentNodeTestSuite))
}
