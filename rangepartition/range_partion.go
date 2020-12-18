package rangepartition

import (
	"context"
	"io"
	"time"

	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
	"github.com/pkg/errors"
)

type RangePartition struct {
	metaStream *streamclient.StreamClient //设定metadata结构
	rawStream  *streamclient.StreamClient //设定checkpoint结构
	blobStream *streamclient.StreamClient //to be GC
	sm         *smclient.SMClient
	commitLog  *CommitLog
	tableType  string
}

func NewRangePartition(sm *smclient.SMClient, tableType string, metaStreamID uint64) *RangePartition {
	metaStream := streamclient.NewStreamClient(sm, metaStreamID, 1)
	return &RangePartition{
		metaStream: metaStream,
		sm:         sm,
	}
}

func extractMetadata(block *pb.Block) (pspb.MetaStreamData, error) {
	var mix pspb.MixedLog
	var meta pspb.MetaStreamData
	utils.Check(mix.Unmarshal(block.UserData))
	if len(mix.Offsets) != 2 {
		return pspb.MetaStreamData{}, errors.Errorf("block doesn't have metadata")
	}
	size := mix.Offsets[1] - mix.Offsets[0]
	utils.Check(meta.Unmarshal(block.Data[:size]))
	return meta, nil
}

//read metaStream, connect commitlog, rawDataStreamand blobDataStream
func (rp *RangePartition) Connect() error {
	var err error
	if err = rp.metaStream.Connect(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	reader := rp.metaStream.NewSeqReader()
	blocks, err := reader.Read(ctx)
	cancel()
	if err != nil && err != io.EOF {
		return err
	}
	if len(blocks) != 1 {
		return errors.Errorf("failed to get metadata")
	}
	meta, err := extractMetadata(blocks[0])
	if err != nil {
		return err
	}
	//connect to logStream
	rp.commitLog, err = NewCommitLog(meta.LogStreamID, rp.sm)
	if err != nil {
		return err
	}
	//connect to rawdataStream
	rp.rawStream = streamclient.NewStreamClient(rp.sm, meta.RawStreamID, 1)
	if err = rp.rawStream.Connect(); err != nil {
		return err
	}

	//connect to blobStream

	return nil
}

//k,v,blob
//services
func (rp *RangePartition) WriteRPC(key, value string) error {
	/*
		ch := make(chan struct{})
		callback := func(entry *pspb.LogEntry, extendId uint64, offset uint32, innerOffset uint32, err error) {
			fmt.Printf("extentID: %d, offset: %d, innerOffset: %d, err :%v\n", extendId, offset, innerOffset, err)
			rp.memStore, _, _ = rp.memStore.Insert([]byte(key), value)
			close(ch)
		}
		rp.commitLog.Append(&pspb.LogEntry{
			Key:   key,
			Value: []byte(value),
		}, callback)
		//block until finished
		<-ch
	*/
	return nil
}
