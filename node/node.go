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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/extent"
	smclient "github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"golang.org/x/net/context"
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

	smClient *smclient.SMClient
}

func NewExtentNode(baseFileDir string, listenUrl string, smAddr []string) *ExtentNode {
	utils.AssertTrue(xlog.Logger != nil)

	return &ExtentNode{
		extentMap:   new(sync.Map),
		replicates:  new(sync.Map),
		baseFileDir: baseFileDir,
		listenUrl:   listenUrl,
		smClient:    smclient.NewSMClient(smAddr),
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

func (en *ExtentNode) RegisterNode() {
	xlog.Logger.Infof("RegisterNode")

	err := en.smClient.Connect()
	if err != nil {
		xlog.Logger.Fatalf(err.Error())
	}

	storeIDPath := path.Join(en.baseFileDir, "node_id")
	idString, err := ioutil.ReadFile(storeIDPath)
	if err == nil {
		id, err := strconv.ParseUint(string(idString), 10, 64)
		if err != nil {
			xlog.Logger.Fatalf("can not read ioString")
		}
		en.nodeID = id
		return
	}

	//if no such file: node_id, registerNode
	sleep := 10 * time.Millisecond
	for loop := 0; ; loop++ {
		id, err := en.smClient.RegisterNode(context.Background(), en.listenUrl)
		if err != nil {
			xlog.Logger.Warnf("can not register myself: %v", err)
			time.Sleep(sleep)
			sleep = 2 * sleep
			if loop > 10 {
				xlog.Logger.Fatal("can not register myself")
			}
			continue
		}
		en.nodeID = id
		break
	}

	if err = ioutil.WriteFile(storeIDPath, []byte(fmt.Sprintf("%d", en.nodeID)), 0644); err != nil {
		xlog.Logger.Fatalf("try to write file %s, %d, but failed, try to save it manually", storeIDPath, en.nodeID)
	}
	xlog.Logger.Infof("success to register to sm")

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
