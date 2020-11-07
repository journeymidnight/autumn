package rangepartition

import (
	"context"
	"io"

	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

/*

commitLog在1ms的时间窗口内, 重新排序输入的log, size小的在前, 小的log
拼成一个大的pb.Block或者多个pb.Block.

这些拼接的pb.Block组成一个OP发送到stream层(调用stream.Apppend()),

最下面的stream也会尝试batch多个OP, 把多个OP拼成一个RPC发送到ExtentNodes

+-----+----+-------------------+-----+------------------+
|     |    |                   |     |                  |    Write LOG
|log1 |log2|    log3           | log4|      log5        |
+----+----+-----+--------------+--+---------------------+
|    |    |     |                 |                     |
|    |    |     |                 |                     |   reorder in commit Log
|log2|log1|log4 |    log3         |       log5          |
+---------+-----+-----------------+---------------------+
|         |                       |                     |   stream Append
|  OP1    |        OP2            |       OP2           |
|         |                       |                     |
+---------+---------+-------------+---------------------+
|         |         |             |                     |
|  block  |  block  |  block      |      block          |   RPC
|         |         |             |                     |
+---------+---------+-------------+---------------------+

*/

type AppendCallback func(entry *pspb.LogEntry, extendId uint64, offset uint32, innerOffset uint32, err error)

type CommitLog struct {
	stream   *streamclient.StreamClient
	streamID uint64
	inputCh  chan LogCommand
	stopper  *utils.Stopper
}

func NewCommitLog(streamID uint64, sm *smclient.SMClient) (*CommitLog, error) {
	utils.AssertTrue(xlog.Logger != nil)

	stream := streamclient.NewStreamClient(sm, streamID, 32)
	if err := stream.Connect(); err != nil {
		return nil, err
	}
	cl := &CommitLog{
		stream:   stream,
		streamID: streamID,
		inputCh:  make(chan LogCommand, 32),
		stopper:  utils.NewStopper(),
	}

	cl.stopper.RunWorker(cl.reorder)
	cl.stopper.RunWorker(cl.getEvents) //run callbacks
	return cl, nil
}

func (cl *CommitLog) getEvents() {
	for {
		select {
		case <-cl.stopper.ShouldStop():
			//drain all inflightIO
			for cl.stream.InfightIO() > 0 {
				<-cl.stream.GetComplete()
			}
			return
		case result := <-cl.stream.GetComplete():
			logCmds := result.UserData.([]LogCommand)
			for _, logCmd := range logCmds {
				if result.Err != nil {
					logCmd.f(logCmd.entry, 0, 0, 0, result.Err)
				} else {
					logCmd.f(logCmd.entry, result.ExtentID, result.Offsets[logCmd.blockNum], logCmd.innerOffset, nil)
				}
			}

		}
	}
}

type LogCommand struct {
	entry       *pspb.LogEntry
	f           AppendCallback
	innerOffset uint32 //记录log在当前block的相对位置
	blockNum    int    //记录log的当前block, 在OP的相对位置
}

//4K
var (
	maxMixedBlockSize = 4 << 10
	//at most (512-8)/4
	maxEntries = 100
)

type mixedBlock struct {
	offsets *pspb.MixedLog
	data    []byte
	tail    int
}

func NewMixedBlock() *mixedBlock {
	return &mixedBlock{
		offsets: new(pspb.MixedLog),
		data:    make([]byte, maxMixedBlockSize, maxMixedBlockSize),
		tail:    0,
	}
}

func (mb *mixedBlock) CanFill(entry *pspb.LogEntry) bool {
	if mb.tail+entry.Size() > maxMixedBlockSize || len(mb.offsets.Offsets) >= maxEntries {
		return false
	}
	return true
}

func (mb *mixedBlock) Fill(entry *pspb.LogEntry) uint32 {
	mb.offsets.Offsets = append(mb.offsets.Offsets, uint32(mb.tail))
	entry.MarshalTo(mb.data[mb.tail:])
	offset := mb.tail
	mb.tail += entry.Size()
	return uint32(offset)
}

func (mb *mixedBlock) ToBlock() *pb.Block {
	mb.offsets.Offsets = append(mb.offsets.Offsets, uint32(mb.tail))
	userData, err := mb.offsets.Marshal()
	utils.Check(err)
	block := &pb.Block{
		BlockLength: uint32(maxMixedBlockSize),
		UserData:    userData,
		CheckSum:    utils.AdlerCheckSum(mb.data),
		Data:        mb.data,
	}
	return block
}

func (cl *CommitLog) reorder() {
	for {
		select {
		case <-cl.stopper.ShouldStop():
			return
		case entry := <-cl.inputCh:
			var logCmds []LogCommand
			size := 0
		slurpLoop:
			for {
				logCmds = append(logCmds, entry)
				size += entry.entry.Size()
				if size > (20 << 20) {
					break slurpLoop
				}
				select {
				case entry = <-cl.inputCh:
				case <-time.After(time.Microsecond):
					break slurpLoop
				}
			}
			xlog.Logger.Debugf("sort entries by size len[%d]", len(logCmds))

			sort.Slice(logCmds, func(i, j int) bool {
				return logCmds[i].entry.Size() < logCmds[i].entry.Size()
			})

			var blocks []*pb.Block
			var mblock *mixedBlock = nil
			i := 0
			for i = 0; i < len(logCmds); i++ {
				if logCmds[i].entry.Size() > maxMixedBlockSize {
					break
				}
				if mblock == nil {
					mblock = NewMixedBlock()
				}
				if !mblock.CanFill(logCmds[i].entry) {
					blocks = append(blocks, mblock.ToBlock())
					mblock = NewMixedBlock()
				}

				//save innerOffset and blockNum
				logCmds[i].innerOffset = mblock.Fill(logCmds[i].entry)
				logCmds[i].blockNum = len(blocks)
			}
			if mblock != nil {
				blocks = append(blocks, mblock.ToBlock())
			}

			for ; i < len(logCmds); i++ {
				//build big variable IO
				blockLength := utils.Ceil(uint32(logCmds[i].entry.Size()), 4096)
				data := make([]byte, blockLength)
				logCmds[i].entry.MarshalTo(data)
				var mix pspb.MixedLog
				mix.Offsets = []uint32{uint32(logCmds[i].entry.Size())}
				blockUserData, err := mix.Marshal()
				utils.Check(err)

				blocks = append(blocks, &pb.Block{
					BlockLength: blockLength,
					UserData:    blockUserData,
					Data:        data,
					CheckSum:    utils.AdlerCheckSum(data),
				})
				logCmds[i].blockNum = len(blocks) - 1
			}

			err := cl.stream.Append(context.Background(), blocks, logCmds)
			utils.Check(err)
		}

	}
}

//non-Block IO
func (cl *CommitLog) Append(entry *pspb.LogEntry, f AppendCallback) {
	cl.inputCh <- LogCommand{
		entry: entry,
		f:     f,
	}
}

//Read is random READ
func (cl *CommitLog) Read(ctx context.Context, extentID uint64, offset uint32, innerOffset uint32) (*pspb.LogEntry, error) {
	blocks, err := cl.stream.Read(ctx, extentID, offset, 1)
	if err != nil {
		return nil, err
	}
	var entry pspb.LogEntry
	var mix pspb.MixedLog
	mix.Unmarshal(blocks[0].UserData)
	utils.Check(err)

	//lower bound of binary search
	idx := sort.Search(len(mix.Offsets), func(i int) bool { return mix.Offsets[i] >= innerOffset })

	if idx == len(mix.Offsets) || mix.Offsets[idx] != innerOffset {
		return nil, errors.Errorf("innerOffset not found in blocks")
	}

	length := mix.Offsets[idx+1] - mix.Offsets[idx]

	err = entry.Unmarshal(blocks[0].Data[innerOffset : innerOffset+length])
	utils.Check(err)
	return &entry, nil
}

type LogIter struct {
	*streamclient.SeqReader
}

func (cl *CommitLog) NewLogIter() LogIter {
	return LogIter{cl.stream.NewSeqReader()}
}

//LogIter.Read could return [entires, io.EOF], be carefull with this
func (iter LogIter) Read() ([]*pspb.LogEntry, error) {
	blocks, err := iter.SeqReader.Read(context.Background())
	if err != nil && err != io.EOF {
		return nil, err
	}
	var ret []*pspb.LogEntry
	for i := range blocks {
		ret = append(ret, extractLogEntry(blocks[i])...)
	}
	return ret, err
}

func extractLogEntry(block *pb.Block) []*pspb.LogEntry {
	var mix pspb.MixedLog
	utils.Check(mix.Unmarshal(block.UserData))
	ret := make([]*pspb.LogEntry, len(mix.Offsets)-1, len(mix.Offsets)-1)
	for i := 0; i < len(mix.Offsets)-1; i++ {
		length := mix.Offsets[i+1] - mix.Offsets[i]
		entry := new(pspb.LogEntry)
		err := entry.Unmarshal(block.Data[mix.Offsets[i] : mix.Offsets[i]+length])
		utils.Check(err)
		ret[i] = entry
	}
	return ret
}

/*
func (cl *CommitLog) Truncate() {

}
*/
