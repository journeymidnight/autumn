package node

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

var (
	_ = fmt.Println
)

func init() {
	xlog.InitLog([]string{"test.log"}, zapcore.DebugLevel)
}


func TestBasicNode(t *testing.T) {
	nodes := make([]*ExtentNode, 3)
	dirs := []string{"store1","store2", "store3"}
	for i, dir := range dirs {
		os.Mkdir(dir, 0744)
		FormatDisk(dir)
		ioutil.WriteFile(dir + "/node_id", []byte(fmt.Sprintf("%d", i+1)), 0644)
	}
	for _, dir := range dirs {
		defer os.RemoveAll(dir)
	}

	for i, dir := range dirs {
		nodes[i] = NewExtentNode(uint64(i+1), []string{dir}, "", fmt.Sprintf("127.0.0.1:330%d",i+1), []string{"127.0.0.1:3401"})
	}


	for _, n := range nodes {
		utils.Check(n.LoadExtents())
		utils.Check(n.ServeGRPC())
	}


	time.Sleep(time.Second)
	for _, n := range nodes {
		res, err := n.AllocExtent(context.Background(), &pb.AllocExtentRequest{
			ExtentID: 100,
		})
		require.Nil(t, err)
		require.Equal(t, pb.Code_OK, res.Code)
	}
	data := make([]byte, 4096)
	utils.SetRandStringBytes(data)

	block := &pb.Block{
		Data:        data,
	}

	_, err := nodes[0].Append(context.Background(), &pb.AppendRequest{
		ExtentID: 100,
		Blocks:   []*pb.Block{block},
		Peers: []string{"127.0.0.1:3301", "127.0.0.1:3302", "127.0.0.1:3303"},
	})

	require.Nil(t, err)

}

