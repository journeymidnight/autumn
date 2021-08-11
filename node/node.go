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
	"strconv"
	"strings"
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


type ExtentOnDisk struct{
	*extent.Extent
	diskID uint64
}

type ExtentNode struct {
	nodeID     uint64
	grcpServer *grpc.Server
	listenUrl  string
	diskFSs    map[uint64]*diskFS //read only after boot-up, so do not have locks so far
	wal        *wal.Wal
	extentMap  *sync.Map //extentID => ExtentOnDisk

	smClient *smclient.SMClient
	em       *smclient.ExtentManager
	recoveryTaskNum  int32
}

func NewExtentNode(nodeID uint64, diskDirs []string, walDir string, listenUrl string, smAddr []string, etcdAddr []string) *ExtentNode {
	utils.AssertTrue(xlog.Logger != nil)

	en := &ExtentNode{
		extentMap: new(sync.Map),
		listenUrl: listenUrl,
		smClient:  smclient.NewSMClient(smAddr),
		nodeID:    nodeID,
		diskFSs: make(map[uint64]*diskFS),
		
	}
	if err := en.smClient.Connect(); err != nil {
		xlog.Logger.Fatal(err)
		return nil
	}
	

	en.em = smclient.NewExtentManager(en.smClient, etcdAddr, en.extentInfoUpdatedfunc)

	//load disk
	for _, diskDir := range diskDirs {
		disk, err := OpenDiskFS(diskDir, en.nodeID)
		if err != nil {
			xlog.Logger.Fatalf("can not load disk %s, [%v]", diskDir, err)
			//FIXME: if one disk failed, can we continue?
		}
		//FIXME: validate dirs
		en.diskFSs[disk.diskID] = disk
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


func (en *ExtentNode) extentInfoUpdatedfunc(eventType string, cur *pb.ExtentInfo, prev *pb.ExtentInfo) {
	switch eventType {
	case "PUT":
		//sealed
		if cur.Avali > 0 {
			ex := en.getExtent(cur.ExtentID)
			if ex != nil && ex.IsSeal() == false {
				xlog.Logger.Infof("SEAL extent %d", cur.ExtentID)
				ex.Lock()
				if err := ex.Seal(uint32(cur.SealedLength)) ;err != nil {
					//if error happend, wait for manager send a ReAvali msg to recovery data
					xlog.Logger.Errorf(err.Error())
				}
				ex.Unlock()
			}

		}
	case "DELETE":
		//FIXME, TODO
		ex := en.getExtent(prev.ExtentID)
		if ex != nil {
			fmt.Printf("you should delete extent %d...", prev.ExtentID)
		}
	}
	
}

func (en *ExtentNode) SyncFs() {
	for _, fs := range en.diskFSs {
		fs.Syncfs()
	}
}

func (en *ExtentNode) getExtent(ID uint64) *ExtentOnDisk {
	v, ok := en.extentMap.Load(ID)
	if !ok {
		return nil
	}
	return v.(*ExtentOnDisk)
}

func (en *ExtentNode) removeExtent(ID uint64) {
	en.extentMap.Delete(ID)
}

func (en *ExtentNode) setExtent(ID uint64, eod *ExtentOnDisk) {
	en.extentMap.Store(ID, eod)
}


func (en *ExtentNode) LoadExtents() error {
	//register each extent to node
	registerExt := func(path string, diskID uint64) {
		ex, err := extent.OpenExtent(path)
		if err != nil {
			xlog.Logger.Error(err)
			return
		}
		en.setExtent(ex.ID, &ExtentOnDisk{
			Extent: ex,
			diskID: diskID,
		})
		xlog.Logger.Debugf("found extent %d", ex.ID)
	}

	registerCopy := func(path string, diskID uint64) {
		//extentID.replaceID.copy
		parts := strings.Split(path, ".")
		if len(parts) != 3 {
			xlog.Logger.Errorf("found extent %s: can not parse replaceID", path)
			return
		}
		//restore task from filename
		extentID , err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			xlog.Logger.Error(err)
			return
		}

		replaceID , err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			xlog.Logger.Error(err)
			return
		}

		task := pb.RecoveryTask{
			ExtentID: extentID,
			ReplaceID: replaceID,
			NodeID: en.nodeID,
		}

		//if extent's version is updated. remove RecoveryTask
		extentInfo := en.em.Latest(extentID)

		go en.runRecoveryTask(&task, extentInfo, path, diskID)
	}

	var wg sync.WaitGroup
	for _, d := range en.diskFSs {
		wg.Add(1)
		disk := d
		go func() {
			disk.LoadExtents(registerExt, registerCopy)
			wg.Done()
		}()
	}
	wg.Wait()

	//read the wal, and replay writes to extents
	replay := func(ID uint64, start uint32, rev int64, data []*pb.Block) {
		ex := en.getExtent(ID)
		if ex == nil {
			xlog.Logger.Warnf("extentID %d not exist", ID)
			return
		}
		ex.RecoveryData(start,rev, data)
		//ex.CommitLength()
	}

	//replay en.wal to recovery
	if en.wal != nil {
		en.wal.Replay(replay)
	}

	en.extentMap.Range(func(k, v interface{}) bool {
		ex := v.(*ExtentOnDisk)
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
		ex := v.(*ExtentOnDisk)
		ex.Close()
		return true
	})
}

func (en *ExtentNode) ServeGRPC() error {
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(64<<20),
		grpc.MaxSendMsgSize(64<<20),
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
func (en *ExtentNode) AppendWithWal(ex *extent.Extent, rev int64, blocks []*pb.Block) ([]uint32, uint32, error) {

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
		err := en.wal.Write(ex.ID, start, rev, blocks)
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
