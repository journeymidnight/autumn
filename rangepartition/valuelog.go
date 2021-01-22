package rangepartition

import (
	"context"
	"io"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/utils"
)

//replay valuelog
//compact valuelog

func extractLogEntry(block *pb.Block) []*pspb.Entry {
	var mix pspb.MixedLog
	utils.Check(mix.Unmarshal(block.UserData))
	ret := make([]*pspb.Entry, len(mix.Offsets)-1, len(mix.Offsets)-1)
	for i := 0; i < len(mix.Offsets)-1; i++ {
		length := mix.Offsets[i+1] - mix.Offsets[i]
		entry := new(pspb.Entry)
		err := entry.Unmarshal(block.Data[mix.Offsets[i] : mix.Offsets[i]+length])
		utils.Check(err)
		ret[i] = entry
	}
	return ret
}

type LogEntryIter struct {
	reader streamclient.SeqReader
	eof    bool
}

/*
usage of iter
for {
	entries, err := iter.Next()
	if err != nil {
		break
	}
	//
	..process entries
	//
}
*/

/*
func CalcBlockPosition([]*pb.blocks, XXX) (v)
*/

func (iter LogEntryIter) Next() ([]*pspb.Entry, error) {
	//因为SeqReader比较怪, 有可能出现[有entries数据,io.EOF],
	//所以这里做一个简单的封装, 保证返回是errors时, []*pspb.Entriy为nil
	if iter.eof {
		return nil, io.EOF
	}
	blocks, err := iter.reader.Read(context.Background())
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(blocks) == 0 {
		return nil, io.EOF
	}
	var ret []*pspb.Entry
	for i := range blocks {
		ret = append(ret, extractLogEntry(blocks[i])...)
	}
	if err == io.EOF {
		iter.eof = true
	}
	return ret, nil
}
