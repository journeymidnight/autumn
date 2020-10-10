/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless  by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package node

import (
	"fmt"
	"io/ioutil"
	"net"
	"path"
	"strings"
	"sync"

	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/node/conn"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"google.golang.org/grpc"
)

var (
	_ = conn.GetPools
	_ = fmt.Printf
)

/*
view about the extents it owns and where the peer replicas are for a given extent
extentID:[nodeID, nodeID, nodeID]
*/
type ExtentNode struct {
	nodeID      uint64
	grcpServer  *grpc.Server
	listenUrl   string
	baseFileDir string
	extentMap   *sync.Map
	//extentMap map[uint64]*extent.Extent //extent it owns: extentID => file
	//TODO: cached SM date in EN
	replicates *sync.Map
	//replicates map[uint64][]string //extentID => [addr1, addr2]
}

func NewExtentNode(baseFileDir string, listenUrl string) *ExtentNode {
	utils.AssertTrue(xlog.Logger != nil)

	return &ExtentNode{
		extentMap:   new(sync.Map),
		replicates:  new(sync.Map),
		baseFileDir: baseFileDir,
		listenUrl:   listenUrl,
	}
}

func (en *ExtentNode) getExtent(ID uint64) *extent.Extent {

	v, ok := en.extentMap.Load(ID)
	if !ok {
		return nil
	}
	return v.(*extent.Extent)
}

func (en *ExtentNode) setExtent(ID uint64, ex *extent.Extent) {
	en.extentMap.Store(ID, ex)
}

func (en *ExtentNode) getReplicates(extentID uint64) []string {
	v, ok := en.replicates.Load(extentID)
	if !ok {
		return nil
	}
	return v.([]string)
}

func (en *ExtentNode) setReplicates(extentID uint64, addrs []string) {
	en.replicates.Store(extentID, addrs)

}

func (en *ExtentNode) RegisterNode() error {
	return nil
}

func (en *ExtentNode) LoadExtents() error {
	fileInfos, err := ioutil.ReadDir(en.baseFileDir)
	if err != nil {
		return err
	}
	for _, info := range fileInfos {
		name := info.Name()
		if strings.HasSuffix(name, ".ext") {
			ext, err := extent.OpenExtent(path.Join(en.baseFileDir, name))
			if err != nil {
				xlog.Logger.Warnf("can not open %s %v", name, err)
				continue
			}
			en.setExtent(ext.ID, ext)
		}
	}
	return nil
}

func (en *ExtentNode) Shutdown() {
	en.grcpServer.Stop()
	//loop over all extent to close
	//Range(f func(key, value interface{}) bool)
	en.extentMap.Range(func(k, v interface{}) bool {
		ex := v.(*extent.Extent)
		ex.Close()
		return true
	})
}

func (en *ExtentNode) ServeGRPC() error {
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(65<<20),
		grpc.MaxSendMsgSize(65<<20),
		grpc.MaxConcurrentStreams(1000),
	)

	pb.RegisterInternalExtentServiceServer(grpcServer, en)
	pb.RegisterExtentServiceServer(grpcServer, en)

	listener, err := net.Listen("tcp", en.listenUrl)
	if err != nil {
		return err
	}
	go func() {
		grpcServer.Serve(listener)
	}()
	en.grcpServer = grpcServer
	return nil
}
