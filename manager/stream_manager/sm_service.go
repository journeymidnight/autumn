package stream_manager

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)


func (sm *StreamManager) CreateStream(ctx context.Context, req *pb.CreateStreamRequest) (*pb.CreateStreamResponse, error) {

	errDone := func(err error) (*pb.CreateStreamResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.CreateStreamResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

	if !sm.AmLeader() {
		return errDone(wire_errors.NotLeader)
	}

	xlog.Logger.Info("alloc new stream")
	//block forever
	start, _, err := sm.allocUniqID(2)
	if err != nil {
		return errDone(err)
	}
	//streamID and extentID.
	streamID := start
	extentID := start + 1

	if req.DataShard == 0 {
		return errDone(errors.New("req.DataShard can not be 0"))
	}
	
	if req.ParityShard > 0 && req.DataShard < 2 {
		return errDone(errors.New("DataShard can not be less than 2 when EC is used"))
	}
	

	nodes := sm.getAllNodeStatus(true)

	nodes, err = sm.policy.AllocExtent(nodes, int(req.DataShard + req.ParityShard), nil)
	if err != nil {
		return errDone(err)
	}

	diskIDs, err := sm.sendAllocToNodes(ctx, nodes, extentID)
	if err != nil {
		return errDone(err)
	}

	//update ETCD
	//new  stream
	streamKey := formatStreamKey(streamID)
	streamInfo := pb.StreamInfo{
		StreamID:  streamID,
		ExtentIDs: []uint64{extentID},
	}

	sdata, err := streamInfo.Marshal()
	utils.Check(err)

	
	nodeIDs := extractNodeId(nodes)

	utils.AssertTrue(len(nodeIDs)==int(req.DataShard) + int(req.ParityShard))
	//new extents
	extentKey := formatExtentKey(extentID)
	extentInfo := pb.ExtentInfo{
		ExtentID:   extentID,
		Replicates: nodeIDs[:req.DataShard],
		Parity: nodeIDs[req.DataShard:],
		Eversion: 1,
		ReplicateDisks: diskIDs[:req.DataShard],
		ParityDisk: diskIDs[req.DataShard:],
		Refs: 1,
	}

	edata, err := extentInfo.Marshal()
	utils.Check(err)

	ops := []clientv3.Op{
		clientv3.OpPut(streamKey, string(sdata)),
		clientv3.OpPut(extentKey, string(edata)),
	}

	err = etcd_utils.EtcdSetKVS(sm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
	}, ops)

	if err != nil {
		return errDone(err)
	}

	//update memory, create stream and extent.

	sm.addExtent(streamID, &extentInfo)

	return &pb.CreateStreamResponse{
		Code:   pb.Code_OK,
		Stream: &streamInfo,
	}, nil
}


func (sm *StreamManager) addDisk(info pb.DiskInfo) {
	sm.disks.Set(info.DiskID, &DiskStatus{
		DiskInfo: info,
	})
}

func (sm *StreamManager) addNode(info pb.NodeInfo) {
	sm.nodes.Set(info.NodeID ,&NodeStatus{
		NodeInfo: info,
	})
}

func (sm *StreamManager) addExtent(streamID uint64, exInfo *pb.ExtentInfo) {
	s, ok := sm.cloneStreamInfo(streamID)
	if ok {
		s.ExtentIDs = append(s.ExtentIDs, exInfo.ExtentID)
		sm.streams.Set(streamID, s)
	} else {
		sm.streams.Set(streamID, &pb.StreamInfo{
			StreamID:  streamID,
			ExtentIDs: []uint64{exInfo.ExtentID},
		})
		//sm.streamsLocks.Store(streamID, new(sync.Mutex))
	}
	sm.extents.Set(exInfo.ExtentID, exInfo)
	sm.extentsLocks.Store(exInfo.ExtentID, new(sync.Mutex))

}

func (sm *StreamManager) hasDuplicateAddr(addr string) bool {
	/*
	sm.nodeLock.RLock()
	defer sm.nodeLock.RUnlock()
	for _, n := range sm.nodes {
		if n.Address == addr {
			return true
		}
	}
	*/

	for kv := range sm.nodes.Iter() {
		ns := kv.Value.(*NodeStatus)
		if ns.Address == addr {
			return true
		}
	}
	return false
}

func (sm *StreamManager) cloneDiskInfo(diskID uint64) (*pb.DiskInfo, bool) {
	d, ok := sm.disks.Get(diskID)
	if !ok {
		return nil, false
	}
	v := d.(*DiskStatus)
	ret := v.DiskInfo //copy diskInfo
	return &ret, true
}


func (sm *StreamManager) cloneStreamInfo(streamID uint64) (*pb.StreamInfo, bool) {
	d, ok := sm.streams.Get(streamID)
	if !ok {
		return nil, false
	}
	v := d.(*pb.StreamInfo)
	return proto.Clone(v).(*pb.StreamInfo), true
}

func (sm *StreamManager) CheckCommitLength(ctx context.Context, req *pb.CheckCommitLengthRequest) (*pb.CheckCommitLengthResponse, error) {
	errDone := func(err error) (*pb.CheckCommitLengthResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.CheckCommitLengthResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

	if !sm.AmLeader() {
		return errDone(wire_errors.NotLeader)
	}

	stream, ok := sm.cloneStreamInfo(req.StreamID)
	if !ok {
		return errDone(errors.Errorf("no such stream %d", req.StreamID))
	}

	tailExtentID := stream.ExtentIDs[len(stream.ExtentIDs) - 1]
	lastExtentInfo, ok := sm.cloneExtentInfo(tailExtentID)
	if !ok {
		return errDone(errors.Errorf("internal errors, no such extent %d", tailExtentID))
	}

	if lastExtentInfo.Avali > 0 {
		return &pb.CheckCommitLengthResponse{
			Code: pb.Code_OK,
			StreamInfo: stream,
			End: uint32(lastExtentInfo.SealedLength),
			LastExInfo: lastExtentInfo,
		},nil
	}

		
	sm.lockExtent(lastExtentInfo.ExtentID)
	defer sm.unlockExtent(lastExtentInfo.ExtentID)


	nodes := sm.getNodes(lastExtentInfo)
	if nodes == nil {
		return errDone(errors.Errorf("request errors, sm.getNodes(lastExtentInfo) == nil"))
	}

	haveToSealLastExtent := false
	//PS invoke 'CheckCommit' which will invoke receiveCommitlength, 
	//update req.Revision to the current PS's revision.
	sizes := sm.receiveCommitlength(ctx, nodes, tailExtentID, req.Revision)


	//examples to haveToSealLastExtent
	//[-1, 10, 10]
	//[-1, -1, -1]
	//[10, 9,  9]
	for i := 0; i < len(sizes); i++ {
		if sizes[i] == -1 || (i > 0 && sizes[i] != sizes[i-1]) {
			haveToSealLastExtent = true
			break
		}
	}

	//all nodes are alive and have the same commit length
	if !haveToSealLastExtent {
		return &pb.CheckCommitLengthResponse{
			Code: pb.Code_OK,
			StreamInfo: stream,
			End: uint32(sizes[0]),
			LastExInfo: lastExtentInfo,
		},nil
	}


	var minSize int
	if len(lastExtentInfo.Parity) > 0  {
		minSize = len(lastExtentInfo.Replicates)
	} else {
		minSize = 1
	}
	var minimalLength int64 = math.MaxInt64

	var avali uint32
	for i := range sizes {
		if sizes[i] != -1 {
			avali |= (1 << i)
			if minimalLength > sizes[i] {
				minimalLength = sizes[i]
			}					
		}
	}
	if bits.OnesCount32(avali) < minSize {
		xlog.Logger.Warnf("avali nodes is %d , less than minSize %d", avali, minSize)
		return errDone(errors.Errorf("avali nodes is %d , less than minSize %d", avali, minSize))
	}


	lastExtentInfo.SealedLength = uint64(minimalLength)
	lastExtentInfo.Eversion ++
	lastExtentInfo.Avali = avali
	ops := []clientv3.Op{clientv3.OpPut(formatExtentKey(lastExtentInfo.ExtentID), string(utils.MustMarshal(lastExtentInfo)))}
	err := etcd_utils.EtcdSetKVS(sm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
		clientv3.Compare(clientv3.CreateRevision(req.OwnerKey), "=", req.Revision),
	}, ops)
	if err != nil {
		return errDone(err)
	}
	
	return &pb.CheckCommitLengthResponse{
		Code: pb.Code_OK,
		StreamInfo: stream,
		End: uint32(minimalLength),
		LastExInfo: lastExtentInfo,
	},nil
}

//doPunchHole will return latest StreamInfo and error
func (sm *StreamManager) doPunchHoles(streamID uint64, extentIDs []uint64, ownerKey string, revision int64) (*pb.StreamInfo, error) {
		//WARNING: streamclient should alway lock stream in PunchHoles, Truncate and AllocExtent

		streamInfo, ok := sm.cloneStreamInfo(streamID)
		if !ok {
			return nil, errors.Errorf("stream %d do not exist", streamID)
		}
		if len(streamInfo.ExtentIDs) == 0 {
			return nil, errors.Errorf("stream %d do not have any extent", streamID)
		}
	
		lastExID := streamInfo.ExtentIDs[len(streamInfo.ExtentIDs) - 1]
	
		//build index for req.ExtentIDs, exclude lastExID
		index := make(map[uint64]bool)
		for _, extentID := range extentIDs {
			if extentID != lastExID {
				index[extentID] = true
			}
		}
	
		punchedExtents := make([]uint64, 0)
	
		for i := len(streamInfo.ExtentIDs) - 1; i >= 0; i-- {
			if _, ok := index[streamInfo.ExtentIDs[i]]; ok {
				punchedExtents = append(punchedExtents, streamInfo.ExtentIDs[i])
				streamInfo.ExtentIDs = append(streamInfo.ExtentIDs[:i], streamInfo.ExtentIDs[i+1:]...)
			}
		}
	
		if len(extentIDs) != len(punchedExtents) {
			xlog.Logger.Warn("punch holes: some extentIDs are not in streamInfo.ExtentIDs")
		}
	
	
		//update streamInfo
		ops := []clientv3.Op{clientv3.OpPut(formatStreamKey(streamID), string(utils.MustMarshal(streamInfo)))}
	
	
	
		//ignore  lock and cloneExtentInfo errors
		for i := range punchedExtents {
			if err := sm.lockExtent(punchedExtents[i]) ; err != nil {
				for j := 0 ; j < i ; j++ {
					sm.unlockExtent(punchedExtents[j])
				}
				return nil, err
			}
		}
		defer func(){
			for i := range punchedExtents {
				sm.unlockExtent(punchedExtents[i])
			}
		}()
	
		//delete extentIDs if ref == 1
		exToBeDeleted := make([]*pb.ExtentInfo, 0)
		exToBeUpdated := make([]*pb.ExtentInfo, 0)
		for i := range punchedExtents {
			punchExInfo, ok := sm.cloneExtentInfo(punchedExtents[i])
			if !ok {
				return nil, errors.Errorf("punch holes: extent %d do not exist", punchedExtents[i])
			}
			if punchExInfo.Avali == 0 {
				return nil, errors.Errorf("punch holes: extent %d should be sealed", punchExInfo.ExtentID)
			}
			if punchExInfo.Refs == 1 {
				exToBeDeleted = append(exToBeDeleted, punchExInfo)
			} else {
				punchExInfo.Refs --
				punchExInfo.Eversion ++
				exToBeUpdated = append(exToBeUpdated, punchExInfo)
			}
		}
	
		//delete extentIDs
		for _, exInfo := range exToBeDeleted {
			ops = append(ops, clientv3.OpDelete(formatExtentKey(exInfo.ExtentID)))
		}
		for _, exInfo := range exToBeUpdated {
			ops = append(ops, clientv3.OpPut(formatExtentKey(exInfo.ExtentID), string(utils.MustMarshal(exInfo))))
		}
	
		err := etcd_utils.EtcdSetKVS(sm.client, []clientv3.Cmp{
			clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
			clientv3.Compare(clientv3.CreateRevision(ownerKey), "=", revision),
		}, ops)
	
	
		if err != nil {
			return nil, err
		}
	
		//change memory
		for _, exInfo := range exToBeUpdated {
			sm.extents.Set(exInfo.ExtentID, exInfo)
		}
		for _, exInfo := range exToBeDeleted {
			sm.extents.Del(exInfo.ExtentID)
			sm.extentsLocks.Delete(exInfo.ExtentID)
		}
		sm.streams.Set(streamID, streamInfo)
		return streamInfo, nil
	
}

func (sm *StreamManager) StreamPunchHoles(ctx context.Context, req *pb.PunchHolesRequest) (*pb.PunchHolesResponse, error) {
	errDone := func(err error) (*pb.PunchHolesResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.PunchHolesResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

	if !sm.AmLeader() {
		return errDone(wire_errors.NotLeader)
	}


	streamInfo, err := sm.doPunchHoles(req.StreamID, req.ExtentIDs, req.OwnerKey, req.Revision)
	if err != nil {
		return errDone(err)
	}

	return &pb.PunchHolesResponse{
		Code: pb.Code_OK,
		Stream: streamInfo,
	}, nil

}

func (sm *StreamManager) StreamAllocExtent(ctx context.Context, req *pb.StreamAllocExtentRequest) (*pb.StreamAllocExtentResponse, error) {

	errDone := func(err error) (*pb.StreamAllocExtentResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.StreamAllocExtentResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

	if !sm.AmLeader() {
		return errDone(wire_errors.NotLeader)
	}

	//WARNING: streamclient should alway lock stream in PunchHoles, Truncate and AllocExtent

	
	streamInfo, ok := sm.cloneStreamInfo(req.StreamID)
	if !ok {
		return errDone(errors.Errorf("no such stream %d", req.StreamID))
	}

	if !ok {
		return errDone(errors.Errorf("no discards for %d", req.StreamID))
	}

	tailExtentID := streamInfo.ExtentIDs[len(streamInfo.ExtentIDs) - 1]
	lastExInfo, ok := sm.cloneExtentInfo(tailExtentID)
	if !ok {
		return errDone(errors.Errorf("internal errors, no such extent %d", tailExtentID))
	}

	
	//version match: req.Sversion == s.Sversion
	sm.lockExtent(lastExInfo.ExtentID)
	defer sm.unlockExtent(lastExInfo.ExtentID)
	

	nodes := sm.getNodes(lastExInfo)
	if nodes == nil {
		return errDone(errors.Errorf("request errors, internal error can not get nodesStatus from extentInfo %d", lastExInfo.ExtentID))
	}


	dataShards := len(lastExInfo.Replicates)
	parityShards := len(lastExInfo.Parity)

	var sizes []int64

	var ops []clientv3.Op
	var minimalLength int64 = math.MaxInt64
	var avali uint32
	//seal the oldExtent, update lastExtentInfo
	if req.End == 0 {
		//recevied commit length
		var minSize int
		//if EC, we have to get datashards results to decide the minimal length
		//if replicated, only one returns can work, but if we want the system more stable
		//use 2 insdead of 1
		if len(lastExInfo.Parity) > 0  {
			minSize = len(lastExInfo.Replicates)
		} else {
			minSize = 1
		}
		
		sizes = sm.receiveCommitlength(ctx, nodes, lastExInfo.ExtentID, req.Revision)
		
		for i := range sizes {
			if sizes[i] != -1 {
				avali |= (1 << i)
				if minimalLength > sizes[i] {
					minimalLength = sizes[i]
				}					
			}
		}
		if bits.OnesCount32(avali) < minSize {
			xlog.Logger.Warnf("avali nodes is %d , less than minSize %d", avali, minSize)
			return errDone(errors.Errorf("avali nodes is %d , less than minSize %d", avali, minSize))
		}
	} else {
		//all nodes were avali
		minimalLength = int64(req.End)
		avali = (1 << (dataShards+ parityShards)) - 1
	}
		
	//set extent
	lastExInfo.SealedLength = uint64(minimalLength)
	lastExInfo.Eversion ++
	lastExInfo.Avali = avali

	data := utils.MustMarshal(lastExInfo)
	
	ops = append(ops, 
		clientv3.OpPut(formatExtentKey(lastExInfo.ExtentID), string(data)),
	)
	

	//alloc new extent
	extentID, _, err := sm.allocUniqID(1)
	if err != nil {
		return errDone(errors.Errorf("can not alloc a new id"))
	}

	nodes = sm.getAllNodeStatus(true)

	//? todo
	nodes, err = sm.policy.AllocExtent(nodes, dataShards+parityShards, nil)
	if err != nil {
		return errDone(err)
	}

	diskIDs, err := sm.sendAllocToNodes(ctx, nodes, extentID); 
	if err != nil {
		return errDone(err)
	}

	//update etcd
	stream, ok := sm.cloneStreamInfo(req.StreamID)
	utils.AssertTrue(ok)

	stream.ExtentIDs = append(stream.ExtentIDs, extentID)
	//add extentID to stream
	streamKey := formatStreamKey(req.StreamID)
	sdata, err := stream.Marshal()
	utils.Check(err)

	nodeIDs := extractNodeId(nodes)
	//new extents
	extentKey := formatExtentKey(extentID)
	newExInfo := pb.ExtentInfo{
		ExtentID:   extentID,
		Replicates: nodeIDs[:dataShards],
		Parity: nodeIDs[dataShards:],
		Eversion: 1,
		ReplicateDisks: diskIDs[:dataShards],
		ParityDisk: diskIDs[dataShards:],
		Refs: 1,
	}

	//set old

	edata, err := newExInfo.Marshal()
	utils.Check(err)

	ops = append(ops, 
		clientv3.OpPut(streamKey, string(sdata)),
		clientv3.OpPut(extentKey, string(edata)),
	)

	err = etcd_utils.EtcdSetKVS(sm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
		clientv3.Compare(clientv3.CreateRevision(req.OwnerKey), "=", req.Revision),
	}, ops)

	if err != nil {
		return errDone(err)
	}

	//update memory
	//update last extent, lastExtent is not sealed, so we do not need to lock
	sm.extents.Set(lastExInfo.ExtentID, lastExInfo)
	//update streams and new extents
	sm.addExtent(req.StreamID, &newExInfo)

	return &pb.StreamAllocExtentResponse{
		Code: pb.Code_OK,
		StreamInfo: stream,
		LastExInfo: &newExInfo,
	}, nil
}

//sealExtents could be all failed.
/*
func (sm *StreamManager) sealExtents(ctx context.Context, nodes []*NodeStatus, extentID uint64, commitLength uint32, revision int64) {
	stopper := utils.NewStopper()
	for _, node := range nodes {
		addr := node.Address
		stopper.RunWorker(func() {
			pool := conn.GetPools().Connect(addr)
			if pool == nil {
				return
			}
			conn := pool.Get()
			c := pb.NewExtentServiceClient(conn)
			pctx, cancel := context.WithTimeout(ctx, 5 * time.Second)
			_, err := c.Seal(pctx, &pb.SealRequest{
				ExtentID:     extentID,
				CommitLength: commitLength,
				Revision: revision,
			})
			cancel()
			if err != nil { //timeout or other error
				xlog.Logger.Warnf(err.Error())
				return
			}
		})
	}
	//save sealed information and commitLength into extentInfo
}
*/

//receiveCommitlength returns minimal commitlength and all node who give us response and who did not
//-1 if no response
func (sm *StreamManager) receiveCommitlength(ctx context.Context, nodes []*NodeStatus, extentID uint64, revision int64) ([]int64) {

	pctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	stopper := utils.NewStopper()
	result := make([]int64, len(nodes))
	for i, node := range nodes {
		addr := node.Address
		j := i
		stopper.RunWorker(func() {
			pool := conn.GetPools().Connect(addr)

			if pool == nil {
				result[j] = -1
				fmt.Println("no result")
				return
			}

			conn := pool.Get()

			c := pb.NewExtentServiceClient(conn)
			
			res, err := c.CommitLength(pctx, &pb.CommitLengthRequest{
				ExtentID: extentID,
				Revision: revision,
			})

			if err != nil { //timeout or other error
				xlog.Logger.Warnf(err.Error())
				result[j] = -1
				return
			}
			result[j] = int64(res.Length)			
		})
	}
	stopper.Wait()
	xlog.Logger.Debugf("get commitlenght of extent %d, the sizes is %+v", extentID, result)
	fmt.Printf("extent/%d receive commmit length: %v\n", extentID, result)

	return result
}

func (sm *StreamManager) sendAllocToNodes(ctx context.Context, nodes []*NodeStatus, extentID uint64) ([]uint64 , error) {
	pctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	stopper := utils.NewStopper()
	n := int32(len(nodes))

	diskIDs := make([]uint64, len(nodes))
	var complets int32
	for i, node := range nodes {
		conn := node.GetConn()
		j := i
		stopper.RunWorker(func() {
			if conn == nil {
				return
			}
			c := pb.NewExtentServiceClient(conn)
			res, err := c.AllocExtent(pctx, &pb.AllocExtentRequest{
				ExtentID: extentID,
			})
			if err != nil || res.Code != pb.Code_OK {
				xlog.Logger.Warnf(err.Error())
				return
			}
			diskIDs[j] = res.DiskID
			atomic.AddInt32(&complets, 1)
		})
	}
	stopper.Wait()
	if complets != n || !sm.AmLeader() {
		return nil, errors.Errorf("not to create stream")
	}
	return diskIDs, nil
}

func (sm *StreamManager) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	
	errDone := func(err error) (*pb.RegisterNodeResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.RegisterNodeResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

	if !sm.AmLeader() {
		return errDone(wire_errors.NotLeader)
	}

	if sm.hasDuplicateAddr(req.Addr) {
		return errDone(errors.New("duplicated addr"))
	}

	//TODO: duplicated disk UUID?

	id, _, err := sm.allocUniqID(uint64(1+len(req.DiskUUIDs)))
	if err != nil {
		return errDone(errors.New("failed to alloc uniq id"))
	}



	//modify etcd
	//add node and disks
	disks := make([]pb.DiskInfo,len(req.DiskUUIDs))
	for i := range req.DiskUUIDs {
		disks[i] = pb.DiskInfo{
			DiskID: id+uint64(i)+1,
			Online: true,
			Uuid: req.DiskUUIDs[i],
		}
	}
	
	seq := func(start uint64, count int) []uint64 {
		a := make([]uint64, count)
		for i := range a {
			a[i] = start + uint64(i)
		}
		return a
	}

	nodeInfo := &pb.NodeInfo{
		NodeID:  id,
		Address: req.Addr,
		Disks: seq(id+1, len(req.DiskUUIDs)),
	}

	data, err := nodeInfo.Marshal()
	utils.Check(err)
	nodeKey := formatNodeKey(id)
	nodeValue := data

	ops := []clientv3.Op{
		clientv3.OpPut(nodeKey, string(nodeValue)),
	}

	for i := range disks {
		data := utils.MustMarshal(&disks[i])
		ops = append(ops, clientv3.OpPut(
			formatDiskKey(id+1+uint64(i)),
			string(data),
		))
	}



	err = etcd_utils.EtcdSetKVS(sm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
	}, ops)
	if err != nil {
		return errDone(err)
	}

	//modify memory
	for i := range disks {
		sm.addDisk(disks[i])
	}
	sm.addNode(*nodeInfo)
	
	uuidToDiskID := make(map[string]uint64)
	for _, disk := range disks {
		uuidToDiskID[disk.Uuid] = disk.DiskID
	}
	return &pb.RegisterNodeResponse{
		Code:   pb.Code_OK,
		NodeId: id,
		DiskUUIDs: uuidToDiskID,
	}, nil
}

func (sm *StreamManager) NodesInfo(ctx context.Context, req *pb.NodesInfoRequest) (*pb.NodesInfoResponse, error) {
	if !sm.AmLeader() {
		return nil, errors.Errorf("not a leader")
	}
	return &pb.NodesInfoResponse{
		Code:  pb.Code_OK,
		Nodes: sm.cloneNodesInfo(),
	}, nil
}

func (sm *StreamManager) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	return &pb.StatusResponse{
		Code: pb.Code_OK,
	}, nil
}


func (sm *StreamManager) ExtentInfo(ctx context.Context, req *pb.ExtentInfoRequest) (*pb.ExtentInfoResponse, error) {
	
	errDone := func(err error) (*pb.ExtentInfoResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.ExtentInfoResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

	if !sm.AmLeader() {
		return errDone(wire_errors.NotLeader)
	}


	exInfo, ok := sm.cloneExtentInfo(req.ExtentID)

	if !ok {
		return errDone(wire_errors.NotFound)
	}
	return &pb.ExtentInfoResponse{
		Code: pb.Code_OK,
		ExInfo: exInfo,
	}, nil


}


func (sm *StreamManager) StreamInfo(ctx context.Context, req *pb.StreamInfoRequest) (*pb.StreamInfoResponse, error) {
	
	errDone := func(err error) (*pb.StreamInfoResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.StreamInfoResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

	if !sm.AmLeader() {
		return errDone(wire_errors.NotLeader)
	}

	reqStreams := req.StreamIDs
	if reqStreams == nil {
		for kv := range sm.streams.Iter() {
			reqStreams = append(reqStreams, kv.Key.(uint64))
		}
	}

	resStreams := make(map[uint64]*pb.StreamInfo)
	resExtents := make(map[uint64]*pb.ExtentInfo)

	var ok bool
	for _, streamID := range reqStreams {
		resStreams[streamID], ok = sm.cloneStreamInfo(streamID)
		if !ok {
			continue
		}
		for _, extentID := range resStreams[streamID].ExtentIDs {
			resExtents[extentID], ok = sm.cloneExtentInfo(extentID)
		}
	}
	return &pb.StreamInfoResponse{
		Code:    pb.Code_OK,
		Streams: resStreams,
		Extents: resExtents,
	}, nil

}

func (sm *StreamManager) Truncate(ctx context.Context, req *pb.TruncateRequest) (*pb.TruncateResponse, error) {
	
	errDone := func(err error) (*pb.TruncateResponse, error){
		code, desCode := wire_errors.ConvertToPBCode(err)
		return &pb.TruncateResponse{
			Code: code,
			CodeDes: desCode,
		}, nil
	}

	if !sm.AmLeader() {
		return errDone(wire_errors.NotLeader)
	}


	//WARNING: streamclient should alway lock stream in PunchHoles, Truncate and AllocExtent

	streamInfo, ok := sm.cloneStreamInfo(req.StreamID)
	if !ok {
		return errDone(errors.Errorf("stream do not have extent %d", req.ExtentID))
	}


	//streamInfo.Sversion  == req.Sversion
	var i int
	for i = range streamInfo.ExtentIDs {
		if streamInfo.ExtentIDs[i] == req.ExtentID {
			break
		}
	}

	if i == 0 {
		return errDone(errors.Errorf("do not have to truncate"))
	}

	oldExtentIDs := streamInfo.ExtentIDs[:i]

	newStreamInfo, err := sm.doPunchHoles(req.StreamID, oldExtentIDs, req.OwnerKey, req.Revision)


	if err != nil {
		return errDone(err)
	}

	return &pb.TruncateResponse{
		Code: pb.Code_OK,
		UpdatedStreamInfo: newStreamInfo,
	}, nil

}


func (sm *StreamManager) cloneExtentInfo(extentID uint64) (*pb.ExtentInfo, bool) {
	d, ok := sm.extents.Get(extentID)
	if !ok {
		return nil, ok
	}
	v := d.(*pb.ExtentInfo)
	return proto.Clone(v).(*pb.ExtentInfo), true
}


func (sm *StreamManager) getNodes(exInfo *pb.ExtentInfo) []*NodeStatus {
	var ret []*NodeStatus

	for _, nodeID := range exInfo.Replicates {
		d, ok := sm.nodes.Get(nodeID)
		if !ok {
			return nil
		}
		ns := d.(*NodeStatus)
		ret = append(ret, ns)
	}
	for _, nodeID := range exInfo.Parity {
		d, ok := sm.nodes.Get(nodeID)
		if !ok {
			return nil
		}
		ns := d.(*NodeStatus)
		ret = append(ret, ns)
	}
	return ret

}


func (sm *StreamManager) getDiskStatus(diskID uint64) *DiskStatus {
	v, ok  := sm.disks.Get(diskID)
	if !ok {
		return nil
	}
	return v.(*DiskStatus)
}

func (sm *StreamManager) getNodeStatus(nodeID uint64) *NodeStatus {
	v, ok  := sm.nodes.Get(nodeID)
	if !ok {
		return nil
	}
	return v.(*NodeStatus)
}

func (sm *StreamManager) getAllNodeStatus(onlyAlive bool) (ret []*NodeStatus) {
	for kv := range sm.nodes.Iter() {
		ns := kv.Value.(*NodeStatus)
		if onlyAlive {
			if ns.IsHealthy() {
				ret = append(ret, ns)
			}
		} else {
			ret = append(ret, ns)

		}
	}
	return ret
}

func (sm *StreamManager) cloneNodesInfo() map[uint64]*pb.NodeInfo {
	ret := make(map[uint64]*pb.NodeInfo)
	for kv := range sm.nodes.Iter() {
		ns := kv.Value.(*NodeStatus)
		ret[ns.NodeID] = &ns.NodeInfo
	}
	return ret
}


func extractNodeId(nodes []*NodeStatus) []uint64 {
	var ret []uint64
	for _, node := range nodes {
		ret = append(ret, node.NodeID)
	}
	return ret
}

func formatStreamKey(ID uint64) string {
	return fmt.Sprintf("streams/%d", ID)
}

func formatNodeKey(ID uint64) string {
	return fmt.Sprintf("nodes/%d", ID)
}

func formatExtentKey(ID uint64) string {
	return fmt.Sprintf("extents/%d", ID)
}

func formatDiskKey(ID uint64) string {
	return fmt.Sprintf("disks/%d", ID)
}

func parseKey(s string, prefix string) (uint64, error) {

	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return 0, errors.Errorf("parse key[%s] failed :", s)
	}
	if parts[0] != prefix {
		return 0, errors.Errorf("parse key[%s] failed, parts[0] not match :", s)
	}
	return strconv.ParseUint(parts[1], 10, 64)
}


//streamclient should lock before call punchholes/truncate/mustAllocwrite
/*
func (sm *StreamManager) lockStream(streamID uint64) error {
	v, ok := sm.streamsLocks.Load(streamID)
	if !ok {
		return errors.Errorf("lock failed: stream %d not found", streamID)
	}
	v.(*sync.Mutex).Lock()
	return nil
}

func (sm *StreamManager) unlockStream(streamID uint64){
	v, ok := sm.streamsLocks.Load(streamID)
	if ok {
		v.(*sync.Mutex).Unlock()
	}		
}
*/