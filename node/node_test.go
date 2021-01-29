package node

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/stretchr/testify/assert"
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
	os.Mkdir("xnodestore1", 0744)
	os.Mkdir("xnodestore2", 0744)
	os.Mkdir("xnodestore3", 0744)

	nodes[0] = NewExtentNode("xnodestore1", "127.0.0.1:3301", []string{"127.0.0.1:3401"})
	nodes[1] = NewExtentNode("xnodestore2", "127.0.0.1:3302", []string{"127.0.0.1:3401"})
	nodes[2] = NewExtentNode("xnodestore3", "127.0.0.1:3303", []string{"127.0.0.1:3401"})

	defer os.RemoveAll("xnodestore1")
	defer os.RemoveAll("xnodestore2")
	defer os.RemoveAll("xnodestore3")

	for _, n := range nodes {
		n.LoadExtents()
		n.ServeGRPC()
	}

	time.Sleep(time.Second)
	for _, n := range nodes {
		res, err := n.AllocExtent(context.Background(), &pb.AllocExtentRequest{
			ExtentID: 100,
		})
		assert.Nil(t, err)
		assert.Equal(t, pb.Code_OK, res.Code)
	}
	data := make([]byte, 4096)
	utils.SetRandStringBytes(data)

	block := &pb.Block{
		CheckSum:    utils.AdlerCheckSum(data),
		BlockLength: 4096,
		Data:        data,
	}

	//nodes[0].setReplicates(100, []string{"127.0.0.1:3302", "127.0.0.1:3303"})
	res, err := nodes[0].Append(context.Background(), &pb.AppendRequest{
		ExtentID: 100,
		Blocks:   []*pb.Block{block},
	})
	assert.Nil(t, err)
	assert.Equal(t, uint32(512), res.Offsets[0])

	//_, err = node1.ReadBlocks(context.Background(), &pb.ReadBlocksRequest{ExtentID: 100, Offsets: []uint32{512}})

}
