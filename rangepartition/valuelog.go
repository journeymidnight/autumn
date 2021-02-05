package rangepartition

import (
	"context"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/streamclient"
)

//replay valuelog
//compact valuelog
//FIXME, 读/写下降到streamlayer
//如果request有多个Entry, 还需要修改/FIXME
func writeValueLog(logStream streamclient.StreamClient, reqs []*request) ([]*pb.EntryInfo, valuePointer, error) {

	var entries []*pb.EntryInfo

	for _, req := range reqs {
		entries = append(entries, req.entries...)
	}

	extentID, offset, err := logStream.AppendEntries(context.Background(), entries)
	if err != nil {
		return nil, valuePointer{}, err
	}
	return entries, valuePointer{extentID, offset}, nil
}

func replayLog(logStream streamclient.StreamClient, startExtentID uint64, startOffset uint32, replayFunc func(*pb.EntryInfo) bool) error {
	opt := streamclient.ReadOption{}.WithReplay()
	if startOffset == 0 && startExtentID == 0 {
		opt = opt.WithReadFromStart()
	} else {
		opt = opt.WithReadFrom(startExtentID, startOffset)
	}
	iter := logStream.NewLogEntryIter(opt)
	for {
		ok, err := iter.HasNext()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		ei := iter.Next()
		if replayFunc(ei) == false {
			break
		}
	}
	return nil
}
