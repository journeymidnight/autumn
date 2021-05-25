package stream_manager

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/journeymidnight/autumn/conn"
	"github.com/journeymidnight/autumn/manager"
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

	err = sm.sendAllocToNodes(ctx, nodes, extentID)
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

	
	nodesID := extractNodeId(nodes)

	utils.AssertTrue(len(nodesID)==int(req.DataShard) + int(req.ParityShard))
	//new extents
	extentKey := formatExtentKey(extentID)
	extentInfo := pb.ExtentInfo{
		ExtentID:   extentID,
		Replicates: nodesID[:req.DataShard],
		Parity: nodesID[req.DataShard:],
	}

	edata, err := extentInfo.Marshal()
	utils.Check(err)

	ops := []clientv3.Op{
		clientv3.OpPut(streamKey, string(sdata)),
		clientv3.OpPut(extentKey, string(edata)),
	}

	err = manager.EtcdSetKVS(sm.client, []clientv3.Cmp{
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


func (sm *StreamManager) addNode(id uint64, addr string) {
	sm.nodes.Set(id,&NodeStatus{
		NodeInfo: pb.NodeInfo{
			NodeID:  id,
			Address: addr,
		}})
}

func (sm *StreamManager) addExtent(streamID uint64, extent *pb.ExtentInfo) {
	s, ok := sm.cloneStreamInfo(streamID)
	if ok {
		s.ExtentIDs = append(s.ExtentIDs, extent.ExtentID)
	} else {
		sm.streams.Set(streamID, &pb.StreamInfo{
			StreamID:  streamID,
			ExtentIDs: []uint64{extent.ExtentID},
		})
	}
	sm.extents.Set(extent.ExtentID, extent)
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

	if req.DataShard == 0 {
		return errDone(errors.New("req.DataShard can not be 0"))
	}
	if req.ParityShard == 0 && req.DataShard != 3 {
		return errDone(errors.New("replica only support 3 replics"))
	}
	
	//get current Stream's last extent, and find current extents' nodes
	nodes, id, lastExtentInfo, err := sm.getAppendExtentsAddr(req.StreamID)
	if err != nil {
		return errDone(err)
	}
	if id != req.ExtentToSeal {
		return errDone(errors.Errorf("extentID no match %d vs %d", id, req.ExtentToSeal))
	}


	if lastExtentInfo.SealedLength == 0 {
		//recevied commit length
		size := sm.receiveCommitlength(ctx, nodes, req.ExtentToSeal)

		if size == 0 || size == math.MaxUint32 {
			return errDone(errors.New("seal can not get Commitlength"))
		}

		sm.sealExtents(ctx, nodes, req.ExtentToSeal, size)
		//save sealed info and update version number to etcd and update local-cache
		
		//sealedExtentInfo := proto.Clone(lastExtentInfo).(*pb.ExtentInfo)

		lastExtentInfo.SealedLength = uint64(size)
		lastExtentInfo.Eversion ++

		data := utils.MustMarshal(lastExtentInfo)
	
		ops := []clientv3.Op{
			clientv3.OpPut(formatExtentKey(id), string(data)),
		}
		err = manager.EtcdSetKVS(sm.client, []clientv3.Cmp{
			clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
		}, ops)
	
		if err != nil {
			return errDone(errors.Errorf("can not set ETCD"))
		}

		sm.extents.Set(lastExtentInfo.ExtentID, lastExtentInfo)
	}


	//alloc new extend
	extentID, _, err := sm.allocUniqID(1)
	if err != nil {
		return errDone(errors.Errorf("can not alloc a new id"))
	}

	nodes = sm.getAllNodeStatus(true)

	//? todo
	nodes, err = sm.policy.AllocExtent(nodes, int(req.DataShard+ req.ParityShard), nil)
	if err != nil {
		return errDone(err)
	}

	if err = sm.sendAllocToNodes(ctx, nodes, extentID); err != nil {
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

	//new extents
	extentKey := formatExtentKey(extentID)
	extentInfo := pb.ExtentInfo{
		ExtentID:   extentID,
		Replicates: extractNodeId(nodes),
	}

	//set old

	edata, err := extentInfo.Marshal()
	utils.Check(err)

	ops := []clientv3.Op{
		clientv3.OpPut(streamKey, string(sdata)),
		clientv3.OpPut(extentKey, string(edata)),
	}

	err = manager.EtcdSetKVS(sm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
	}, ops)

	if err != nil {
		return errDone(errors.New("update etcd failed"))
	}

	//update memory
	//update streams and extents
	sm.addExtent(req.StreamID, &extentInfo)

	return &pb.StreamAllocExtentResponse{
		Code: pb.Code_OK,
		StreamID: req.StreamID,
		Extent:   &extentInfo,
	}, nil
}

//sealExtents could be all failed.
func (sm *StreamManager) sealExtents(ctx context.Context, nodes []*NodeStatus, extentID uint64, commitLength uint32) {
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

func (sm *StreamManager) receiveCommitlength(ctx context.Context, nodes []*NodeStatus, extentID uint64) uint32 {
	pctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	stopper := utils.NewStopper()
	reCh := make(chan uint32)
	for _, node := range nodes {
		addr := node.Address
		stopper.RunWorker(func() {
			pool := conn.GetPools().Connect(addr)
			conn := pool.Get()
			c := pb.NewExtentServiceClient(conn)
			res, err := c.CommitLength(pctx, &pb.CommitLengthRequest{
				ExtentID: extentID,
			})
			if err != nil { //timeout or other error
				xlog.Logger.Warnf(err.Error())
				reCh <- math.MaxUint32
				return
			}
			reCh <- res.Length
		})
	}
	go func(){
		stopper.Wait()
		close(reCh)
	}()

	//choose minimal of all size
	ret := uint32(math.MaxUint32)
	for size := range reCh {
		if size < ret {
			ret = size
		}
	}

	return ret
}

func (sm *StreamManager) sendAllocToNodes(ctx context.Context, nodes []*NodeStatus, extentID uint64) error {
	pctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	stopper := utils.NewStopper()
	n := int32(len(nodes))
	var complets int32
	for _, node := range nodes {
		conn := node.GetConn()
		stopper.RunWorker(func() {
			if conn == nil {
				return
			}
			c := pb.NewExtentServiceClient(conn)
			_, err := c.AllocExtent(pctx, &pb.AllocExtentRequest{
				ExtentID: extentID,
			})
			if err != nil {
				xlog.Logger.Warnf(err.Error())
				return
			}
			atomic.AddInt32(&complets, 1)
		})
	}
	stopper.Wait()
	if complets != n || !sm.AmLeader() {
		return errors.Errorf("not to create stream")
	}
	return nil
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

	id, _, err := sm.allocUniqID(1)
	if err != nil {
		return errDone(errors.New("failed to alloc uniq id"))
	}
	//modify etcd
	nodeInfo := &pb.NodeInfo{
		NodeID:  id,
		Address: req.Addr,
	}
	data, err := nodeInfo.Marshal()
	utils.Check(err)
	nodeKey := formatNodeKey(id)
	nodeValue := data
	ops := []clientv3.Op{
		clientv3.OpPut(nodeKey, string(nodeValue)),
	}

	err = manager.EtcdSetKVS(sm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
	}, ops)
	if err != nil {
		return errDone(err)
	}

	//modify memory
	sm.addNode(id, req.Addr)
	return &pb.RegisterNodeResponse{
		Code:   pb.Code_OK,
		NodeId: id,
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

	out := make(map[uint64]*pb.ExtentInfo)
	for _, extentId := range req.Extents {
		out[extentId], _ = sm.cloneExtentInfo(extentId)
	}
	return &pb.ExtentInfoResponse{
		Code:    pb.Code_OK,
		Extents: out,
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
		return errDone(errors.Errorf("stream do not have streaminfo"))
	}
	var i int
	for i = range streamInfo.ExtentIDs {
		if streamInfo.ExtentIDs[i] == req.ExtentID {
			break
		}
	}

	if i == 0 {
		return &pb.TruncateResponse{
			Code: pb.Code_OK}, nil
	}

	//update ETCD
	newExtentIDs := streamInfo.ExtentIDs[i:]
	streamKey := formatStreamKey(req.StreamID)
	newStreamInfo := pb.StreamInfo{
		StreamID:  req.StreamID,
		ExtentIDs: newExtentIDs,
	}

	sdata, err := newStreamInfo.Marshal()
	utils.Check(err)

	ops := []clientv3.Op{
		clientv3.OpPut(streamKey, string(sdata)),
	}
	err = manager.EtcdSetKVS(sm.client, []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(sm.leaderKey), "=", sm.memberValue),
	}, ops)

	if err != nil {
		return errDone(err)
	}

	sm.streams.Set(req.StreamID, &newStreamInfo)
	return &pb.TruncateResponse{
		Code: pb.Code_OK}, nil
}


func (sm *StreamManager) cloneExtentInfo(extentID uint64) (*pb.ExtentInfo, bool) {
	d, ok := sm.extents.Get(extentID)
	if !ok {
		return nil, ok
	}
	v := d.(*pb.ExtentInfo)
	return proto.Clone(v).(*pb.ExtentInfo), true
}

func (sm *StreamManager) getAppendExtentsAddr(streamID uint64) ([]*NodeStatus,uint64, *pb.ExtentInfo, error) {

	s, ok := sm.cloneStreamInfo(streamID)
	if !ok {
		return nil, 0, nil, errors.Errorf("no such stream %d", streamID)
	}

	lastExtentID := s.ExtentIDs[len(s.ExtentIDs)-1]


	extInfo, ok := sm.cloneExtentInfo(lastExtentID)
	if !ok {
		return nil, 0, nil, errors.Errorf("no such extentd %d", lastExtentID)
	}
	var ret []*NodeStatus
	for _, nodeID := range extInfo.Replicates {
		d, ok := sm.nodes.Get(nodeID)
		if !ok {
			return nil, 0, nil, errors.Errorf("no such nodeID %d in %v", nodeID, extInfo.Replicates)
		}
		ns := d.(*NodeStatus)
		ret = append(ret, ns)
	}
	return ret, lastExtentID, extInfo, nil
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
