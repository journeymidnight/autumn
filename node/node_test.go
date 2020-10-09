package node

import (
	"fmt"
	"testing"

	"github.com/journeymidnight/streamlayer/xlog"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap/zapcore"
)

var (
	_ = fmt.Println
)

func init() {
	xlog.InitLog([]string{"test.log"}, zapcore.DebugLevel)
}

func (suite *NodeTestSuite) SetupSuite() {
	fmt.Printf("setup node test")
	node1 := NewExtentNode(".", "127.0.0.1:3000")
	node1.LoadExtents()
	node1.ServeGRPC()
	suite.nodes = append(suite.nodes, node1)
}

func (suite *NodeTestSuite) TearDownSuite() {
	fmt.Printf("teardown node test")
}

type NodeTestSuite struct {
	suite.Suite
	nodes []*ExtentNode
}

func TestStoreTestSuite(t *testing.T) {
	suite.Run(t, new(NodeTestSuite))
}
