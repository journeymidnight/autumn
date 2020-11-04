package rangepartition

import (
	"context"
	"sort"
	"time"

	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

type AppendCallback func(entry *pspb.LogEntry, extendId uint64, offset uint32, innerOffset int)

type CommitLog struct {
	stream   *streamclient.StreamClient
	streamID uint64
	inputCh  chan LogCommand
	stopper  *utils.Stopper
}

func NewCommitLog(streamID uint64, sm *smclient.SMClient) *CommitLog {
	utils.AssertTrue(xlog.Logger != nil)

	stream := streamclient.NewStreamClient(sm, streamID, 32)
	if err := stream.Connect(); err != nil {
		return nil
	}
	cl := &CommitLog{
		stream:   stream,
		streamID: streamID,
		inputCh:  make(chan LogCommand, 32),
		stopper:  utils.NewStopper(),
	}

	cl.stopper.RunWorker(cl.reorder)
	cl.stopper.RunWorker(cl.getEvents) //run callbacks
	return cl
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
			//注意: 这个logCmds和上面的logCmds完全不同
			logCmds := result.UserData.([]LogCommand)
			for _, logCmd := range logCmds {
				logCmd.f(logCmd.entry, result.ExtentID, result.Offsets[logCmd.blockNum], logCmd.innerOffset)
			}

		}
	}
}

type LogCommand struct {
	entry       *pspb.LogEntry
	f           AppendCallback
	innerOffset int //记录log在当前block的相对位置
	blockNum    int //记录log的当前block, 在OP的相对位置
}

//4K
var (
	maxMixedBlockSize = 4 << 10
	//at most (512-8)/4
	maxEntries = 100
)

type mixedBlock struct {
	lens *pspb.MixedLog
	data []byte
	tail int
}

func NewMixedBlock() *mixedBlock {
	return &mixedBlock{
		lens: new(pspb.MixedLog),
		data: make([]byte, maxMixedBlockSize, maxMixedBlockSize),
		tail: 0,
	}
}

func (mb *mixedBlock) CanFill(entry *pspb.LogEntry) bool {
	if mb.tail+entry.Size() > maxMixedBlockSize || len(mb.lens.LogLen) >= maxEntries {
		return false
	}
	return true
}

func (mb *mixedBlock) Fill(entry *pspb.LogEntry) int {
	mb.lens.LogLen = append(mb.lens.LogLen, uint32(entry.Size()))
	entry.MarshalTo(mb.data[mb.tail:])
	offset := mb.tail
	mb.tail += entry.Size()
	return offset
}

func (mb *mixedBlock) ToBlock() *pb.Block {
	userData, err := mb.lens.Marshal()
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
			var mblock *mixedBlock
			i := 0
			for i = 0; i < len(logCmds); i++ {
				if logCmds[i].entry.Size() >= (8 << 10) {
					break
				}

				if mblock == nil || !mblock.CanFill(logCmds[i].entry) {
					mblock = NewMixedBlock()
					blocks = append(blocks, mblock.ToBlock())
				}
				//save innerOffset and blockNum
				logCmds[i].innerOffset = mblock.Fill(logCmds[i].entry)
				logCmds[i].blockNum = len(blocks) - 1
			}
			for ; i < len(logCmds); i++ {
				//build big variable IO
				blockLength := utils.Ceil(uint32(logCmds[i].entry.Size()), 4096)
				data := make([]byte, blockLength)
				logCmds[i].entry.MarshalTo(data)
				blocks = append(blocks, &pb.Block{
					BlockLength: blockLength,
					UserData:    nil,
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

/*
func extractLogEntry(block *pb.Block) []pspb.LogEntry {

}

//read from start
func (cl *CommitLog) ReadFrom() (*pspb.LogEntry, error) {

}

func (cl *CommitLog) Truncate() {

}
*/
