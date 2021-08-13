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

	"github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
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
	if req.ParityShard == 0 && req.DataShard != 3 {
		return errDone(errors.New("replica only support 3 replics"))
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
		Sversion: 1,
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
		s.Sversion ++
		sm.streams.Set(streamID, s)
	} else {
		sm.streams.Set(streamID, &pb.StreamInfo{
			StreamID:  streamID,
			ExtentIDs: []uint64{exInfo.ExtentID},
			Sversion: 1,
		})
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

	s, ok := sm.cloneStreamInfo(req.StreamID)
	if !ok {
		return errDone(errors.Errorf("no such stream %d", req.StreamID))
	}

	if req.Sversion > int64(s.Sversion) {
		return errDone(wire_errors.ClientStreamVersionTooHigh)
	}

	//duplicated request?
	var lastExtentInfo *pb.ExtentInfo
	if req.Sversion < int64(s.Sversion) {
		fmt.Printf("req.Sversion:%d < s.Sversion:%d\n", req.Sversion, s.Sversion)
		
		tailExtentID := s.ExtentIDs[len(s.ExtentIDs) - 1]
		lastExtentInfo, ok = sm.cloneExtentInfo(tailExtentID)
		if !ok {
			return errDone(errors.Errorf("internal errors, no such extent %d", tailExtentID))
		}
		return &pb.StreamAllocExtentResponse{
			Code: pb.Code_OK,
			StreamID: req.StreamID,
			Extent:   lastExtentInfo,
		}, nil

	}
	
	//version match: req.Sversion == s.Sversion
	tailExtentID := s.ExtentIDs[len(s.ExtentIDs) - 1]
	lastExtentInfo, ok = sm.cloneExtentInfo(tailExtentID)


	if !ok {
		return errDone(errors.Errorf("internal errors, no such extent %d", tailExtentID))
	}

	
	sm.lockExtent(lastExtentInfo.ExtentID)
	defer sm.unlockExtent(lastExtentInfo.ExtentID)
	

	nodes := sm.getNodes(lastExtentInfo)
	if nodes == nil {
		return errDone(errors.Errorf("request errors, extent %d is not the last", req.ExtentToSeal))
	}


	dataShards := len(lastExtentInfo.Replicates)
	parityShards := len(lastExtentInfo.Parity)

	var sizes []int64
	if req.CheckCommitLength > 0 {
		haveToCreateNewExtent := false
		sizes = sm.receiveCommitlength(ctx, nodes, req.ExtentToSeal)
		for i := range sizes {
			if sizes[i] == -1 {
				haveToCreateNewExtent = true
				break			
			}
		}
		
		//all nodes are good
		if haveToCreateNewExtent == false {
			//get minimun from sizes
			minSize := sizes[0]
			sizesAreDiff := false
			for i := 1; i < len(sizes); i++ {
				if minSize > sizes[i] {
					minSize = sizes[i]
					sizesAreDiff = true
				}
			}
			if sizesAreDiff {
				lastExtentInfo.SealedLength = uint64(minSize)
				lastExtentInfo.Eversion ++
				lastExtentInfo.Avali = (1 << (dataShards+ parityShards)) - 1
				ops := []clientv3.Op{clientv3.OpPut(formatExtentKey(lastExtentInfo.ExtentID), string(utils.MustMarshal(lastExtentInfo)))}
				err := etcd_utils.EtcdSetKVS(sm.client, []clientv3.Cmp{
					clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
					clientv3.Compare(clientv3.CreateRevision(req.OwnerKey), "=", req.Revision),
				}, ops)
				if err != nil {
					return errDone(err)
				}
			}
			return  &pb.StreamAllocExtentResponse{
				Code: pb.Code_OK,
				StreamID: req.StreamID,
				End:uint32(sizes[0]),
			}, nil
		}
	}

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
		if len(lastExtentInfo.Parity) > 0  {
			minSize = len(lastExtentInfo.Replicates)
		} else {
			minSize = 2
		}
		
		if len(sizes) == 0 && req.CheckCommitLength == 0 {
			sizes = sm.receiveCommitlength(ctx, nodes, req.ExtentToSeal)
		}

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
		//all nodes are avali:
		minimalLength = int64(req.End)
		avali = (1 << (dataShards+ parityShards)) - 1
	}
		
	//set extent
	lastExtentInfo.SealedLength = uint64(minimalLength)
	lastExtentInfo.Eversion ++
	lastExtentInfo.Avali = avali

	data := utils.MustMarshal(lastExtentInfo)
	
	ops = append(ops, 
		clientv3.OpPut(formatExtentKey(lastExtentInfo.ExtentID), string(data)),
	)
	


	//alloc new extend
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
	stream.Sversion ++
	//add extentID to stream
	streamKey := formatStreamKey(req.StreamID)
	sdata, err := stream.Marshal()
	utils.Check(err)

	nodeIDs := extractNodeId(nodes)
	//new extents
	extentKey := formatExtentKey(extentID)
	extentInfo := pb.ExtentInfo{
		ExtentID:   extentID,
		Replicates: nodeIDs[:dataShards],
		Parity: nodeIDs[dataShards:],
		Eversion: 1,
		ReplicateDisks: diskIDs[:dataShards],
		ParityDisk: diskIDs[dataShards:],
		Refs: 1,
	}

	//set old

	edata, err := extentInfo.Marshal()
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
		return errDone(errors.New("update etcd failed"))
	}

	//update memory
	//update last extent, lastExtent is not sealed, so we do not need to lock
	sm.extents.Set(lastExtentInfo.ExtentID, lastExtentInfo)
	//update streams and new extents
	sm.addExtent(req.StreamID, &extentInfo)

	return &pb.StreamAllocExtentResponse{
		Code: pb.Code_OK,
		StreamID: req.StreamID,
		Extent:   &extentInfo,
		Sversion: stream.Sversion,
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
func (sm *StreamManager) receiveCommitlength(ctx context.Context, nodes []*NodeStatus, extentID uint64) ([]int64) {

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
			Online: 1,
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


	streamInfo, ok := sm.cloneStreamInfo(req.StreamID)
	if !ok {
		return errDone(errors.Errorf("stream do not have extent %d", req.ExtentID))
	}

	if streamInfo.Sversion < uint64(req.Sversion) {
		return errDone(wire_errors.ClientStreamVersionTooHigh)
	}
	
	var blobs pb.BlobStreams
	if len(req.GabageKey) > 0  {
		data, _, err := etcd_utils.EtcdGetKV(sm.client, req.GabageKey)
		if err != nil {
			return errDone(err)
		}
		if len(data) > 0 {
			utils.MustUnMarshal(data, &blobs)
		}
	}

	//duplicated truncated
	if streamInfo.Sversion > uint64(req.Sversion) {
		return &pb.TruncateResponse{
			Code: pb.Code_OK,
			Blobs: &blobs,
		}, nil
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


	//delete extents
	newExtentIDs := streamInfo.ExtentIDs[i:]
	oldExtentIDs := streamInfo.ExtentIDs[:i]

	streamKey := formatStreamKey(req.StreamID)
	newStreamInfo := pb.StreamInfo{
		StreamID:  req.StreamID,
		ExtentIDs: newExtentIDs,
		Sversion: streamInfo.Sversion+1,
	}

	//update current stream
	sdata, err := newStreamInfo.Marshal()
	utils.Check(err)
	ops := []clientv3.Op{
		clientv3.OpPut(streamKey, string(sdata)),
	}


	if len(req.GabageKey) == 0 {
		//delete extents whose ref == 1
		var exToBeDeleted []*pb.ExtentInfo
		var exToBeUpdated  []*pb.ExtentInfo
		for i := range oldExtentIDs {
			if err := sm.lockExtent(oldExtentIDs[i]) ; err != nil {
				for j := 0 ; j < i ; j++ {
					sm.unlockExtent(oldExtentIDs[j])
				}
				return errDone(err)
			}
			oldExInfo, ok := sm.cloneExtentInfo(oldExtentIDs[i])
			if !ok {
				for j := 0 ; j < i ; j++ {
					sm.unlockExtent(oldExtentIDs[j])
				}
				return errDone(errors.Errorf("not found"))
			}
			if oldExInfo.Refs == 1 {
				exToBeDeleted = append(exToBeDeleted, oldExInfo)
			} else {
				oldExInfo.Refs --
				oldExInfo.Eversion ++
				exToBeUpdated = append(exToBeUpdated, oldExInfo)
			}
		}

		defer func(){
			for i := range oldExtentIDs {
				sm.unlockExtent(oldExtentIDs[i])
			}
		}()
		
		for _, exInfo := range exToBeDeleted {
			ops = append(ops, clientv3.OpDelete(formatExtentKey(exInfo.ExtentID)))
		}
		for _, exInfo := range exToBeUpdated {
			ops = append(ops, clientv3.OpPut(formatExtentKey(exInfo.ExtentID), string(utils.MustMarshal(exInfo))))
		}

	} else {
		//create a new stream, and add it to req.GabageKey, it will run gc in the future.
		var newGabageStreamID uint64
		if newGabageStreamID, _, err = sm.allocUniqID(1) ; err != nil {
			return errDone(err)
		}

		blobs.Blobs[newGabageStreamID] = newGabageStreamID

		sdata := utils.MustMarshal(
			&pb.StreamInfo{
				StreamID:  newGabageStreamID,
				ExtentIDs: oldExtentIDs,
				Sversion: 1,
			},
		)
		//transfer old extent to new stream
		//add add stream to gabages
		ops = append(ops, clientv3.OpPut(formatStreamKey(newGabageStreamID), string(sdata)))
		ops = append(ops, clientv3.OpPut(req.GabageKey, string(utils.MustMarshal(&blobs))))
	}
	

	err = etcd_utils.EtcdSetKVS(sm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
		clientv3.Compare(clientv3.CreateRevision(req.OwnerKey), "=", req.Revision),
	}, ops)


	if err != nil {
		return errDone(err)
	}

	sm.streams.Set(req.StreamID, &newStreamInfo)

	return &pb.TruncateResponse{
		Code: pb.Code_OK,
		Blobs: &blobs,
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
