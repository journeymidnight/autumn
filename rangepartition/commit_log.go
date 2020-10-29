package rangepartition

import (
	"sort"

	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

var ()

type CommitLog struct {
	stream   *streamclient.StreamClient
	streamID uint64
	inputCh  chan pspb.LogEntry
	outputCh chan error
	stopper  *utils.Stopper
}

func NewCommitLog(streamID uint64, sm *smclient.SMClient) *CommitLog {
	utils.AssertTrue(xlog.Logger != nil)

	stream := streamclient.NewStreamClient(sm, streamID)
	if err := stream.Connect(); err != nil {
		return nil
	}
	cl := &CommitLog{
		stream:   stream,
		streamID: streamID,
		inputCh:  make(chan pspb.LogEntry),
		outputCh: make(chan error),
		stopper:  utils.NewStopper(),
	}
	//FI
	cl.stopper.RunWorker(cl.reorder)
	cl.stopper.RunWorker(cl.getEvents)
	return cl
}

func (cl *CommitLog) getResult() {

}

func (cl *CommitLog) getEvents() {
	for {
		select {
		case <-cl.stopper.ShouldStop():
			//drain outputCh and close
			return nil

		}
	}
}

func (cl *CommitLog) reorder() {
	for {
		select {
		case <-cl.stopper.ShouldStop():
			//close logic
			//close inputCh or lock
			//drain inputCh, return error
			return
		case entry := <-cl.inputCh:
			var entries []pspb.LogEntry
			size := 0
		slurpLoop:
			for {
				entries = append(entries, entry)
				size += entry.Size()
				if size > (10 << 20) {
					break slurpLoop
				}
				select {
				case entry = <-cl.inputCh:
				default:
					break slurpLoop
				}
			}
			xlog.Logger.Debugf("sort entries by size len[%d]", len(entries))

			sort.Slice(entries, func(i, j int) bool {
				return entries[i].Size() < entries[i].Size()
			})
			var bigIO int
			var smallBlocks []pb.Block
			for i := 0; i < len(entries); i++ {
				if entries[i].Size() >= (8 << 10) {
					bigIO = i
					break
				}
				//small IOs
			}

			for i := bigIO; i < len(entries); i++ {
				//build big variable IO
			}
		}

	}

}

func (cl *CommitLog) Append(entry *pspb.LogEntry) {
	//lock
	//commitLog.logStream.Append()
	//unlock

	//wait
}

func (cl *CommitLog) ReadFrom() (*pspb.LogEntry, error) {

}

func (cl *CommitLog) Truncate() {

}
