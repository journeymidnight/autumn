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
	"net"
	"sync"

	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/extent/wal"
	smclient "github.com/journeymidnight/autumn/manager/smclient"
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
	nodeID     uint64
	grcpServer *grpc.Server
	listenUrl  string
	diskFSs    []*diskFS
	wal        *wal.Wal
	extentMap  *sync.Map
	//extentMap map[uint64]*extent.Extent //extent it owns: extentID => file
	//TODO: cached SM date in EN
	//replicates *sync.Map
	//replicates map[uint64][]string //extentID => [addr1, addr2]

	smClient *smclient.SMClient
	em       *smclient.ExtentManager
}

func NewExtentNode(nodeID uint64, diskDirs []string, walDir string, listenUrl string, smAddr []string) *ExtentNode {
	utils.AssertTrue(xlog.Logger != nil)

	en := &ExtentNode{
		extentMap: new(sync.Map),
		listenUrl: listenUrl,
		smClient:  smclient.NewSMClient(smAddr),
		nodeID:    nodeID,
	}

	if err := en.smClient.Connect(); err != nil {
		xlog.Logger.Fatal(err)
		return nil
	}

	en.em = smclient.NewExtentManager(en.smClient)

	//load disk
	for _, diskDir := range diskDirs {
		disk, err := OpenDiskFS(diskDir, en.nodeID)
		if err != nil {
			xlog.Logger.Fatalf("can not load disk %s, [%v]", diskDir, err)
			//FIXME: if one disk failed, can we continue?
		}
		//FIXME: validate dirs
		en.diskFSs = append(en.diskFSs, disk)
	}

	//load wal
	if len(walDir) > 0 {
		wal, err := wal.OpenWal(walDir, en.SyncFs)
		if err != nil {
			xlog.Logger.Warnf("can not open waldir :%s", walDir)
		} else {
			en.wal = wal
		}
	}
	return en
}

func (en *ExtentNode) SyncFs() {
	for _, fs := range en.diskFSs {
		fs.Syncfs()
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

/*
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
*/

func (en *ExtentNode) LoadExtents() error {
	//register each extent to node
	register := func(ex *extent.Extent) {
		en.setExtent(ex.ID, ex)
		xlog.Logger.Debugf("found extent %d", ex.ID)
	}

	var wg sync.WaitGroup
	for _, disk := range en.diskFSs {
		wg.Add(1)
		go func() {
			disk.LoadExtents(register)
			wg.Done()
		}()
	}
	wg.Wait()

	//read the wal, and replay writes to extents
	replay := func(ID uint64, start uint32, data []*pb.Block) {
		ex := en.getExtent(ID)
		if ex == nil {
			xlog.Logger.Warnf("extentID %d not exist", ID)
			return
		}
		ex.RecoveryData(start, data)
		//ex.CommitLength()
	}

	//replay en.wal to recovery
	if en.wal != nil {
		en.wal.Replay(replay)
	}

	en.extentMap.Range(func(k, v interface{}) bool {
		ex := v.(*extent.Extent)
		if err := ex.ResetWriter(); err != nil {
			xlog.Logger.Warnf("reset writer %s", err.Error())
		}
		return true
	})

	return nil
}

func (en *ExtentNode) Shutdown() {
	en.grcpServer.Stop()
	//loop over all extent to close

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

	pb.RegisterExtentServiceServer(grpcServer, en)

	listener, err := net.Listen("tcp", en.listenUrl)
	if err != nil {
		return err
	}
	go func() {
		utils.Check(grpcServer.Serve(listener))
	}()
	en.grcpServer = grpcServer

	return nil
}

//AppendWithWal will write wal and extent in the same time.
func (en *ExtentNode) AppendWithWal(ex *extent.Extent, blocks []*pb.Block) ([]uint32, uint32, error) {

	if en.wal == nil || utils.SizeOfBlocks(blocks) > (2<<20) {
		//force sync write
		return ex.AppendBlocks(blocks, true)
	}

	var wg sync.WaitGroup
	wg.Add(2) //2 tasks

	errC := make(chan error)
	start := ex.CommitLength()
	var offsets []uint32
	var end uint32
	var err error
	go func() { //write wal
		defer wg.Done()
		err := en.wal.Write(ex.ID, start, blocks)
		errC <- err
	}()

	go func() { //write extent
		defer wg.Done()
		//wal is sync write, do no have to sync
		offsets, end, err = ex.AppendBlocks(blocks, false)
		errC <- err
	}()

	go func() {
		wg.Wait()
		close(errC)
	}()

	for err := range errC {
		if err != nil {
			return nil, 0, err
		}
	}
	return offsets, end, nil
}
