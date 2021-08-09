package erasure_code

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/wire_errors"
	"github.com/klauspost/reedsolomon"
)

type ReedSolomon struct{} 

func (ReedSolomon) Reconstruct(input []io.Reader, dataShards int, parityShards int, output []io.Writer) error{
	enc, err := reedsolomon.NewStream(dataShards, parityShards)
	if err != nil {
		return err
	}	
	if err = enc.Reconstruct(input, output) ; err != nil {
			return err
	}
		
	return nil
}

const (
	metaSize = 4
)

func (ReedSolomon) Decode(input [][]byte, dataShards int, parityShards int) ([]byte, error) {

	enc, err := reedsolomon.New(int(dataShards), int(parityShards))
	if err != nil {
		return nil, err
	}


	// Verify the shards
	ok, err := enc.Verify(input)
	if !ok {
		err = enc.Reconstruct(input)
			if err != nil {
				return nil, err
			}
	}

	perShards := len(input[0])
	size := binary.BigEndian.Uint32(input[dataShards-1][perShards-4:])

	ret := make([]byte, size)
	p := ret
	remaining := size
	for i := 0 ;i < dataShards && remaining > 0 ; i ++ {
		n := utils.Min(int(remaining), perShards)
		copy(p, input[i][:n])
		p = p[n:]
		remaining -= uint32(n)
	}
	return ret, nil
}

func (ReedSolomon) Encode(input []byte, dataShards int, parityShards int) ([][]byte, error) {

	enc, err := reedsolomon.New(int(dataShards), int(parityShards))
	if err != nil {
		return nil, err
	}
	

	size := len(input)
	/*
	leftSpace和perShard至少是4, 保证存储长度
	solution1:
	perShard := (size + dataShards - 1) / dataShards

	//at least 4 bytes to save length
	if perShard < 4 {
		perShard = 4
	}

	leftSpace := perShard * dataShards - size
	
	if leftSpace < 4 {
		perShard += (4-leftSpace) / dataShards + 1
	}
	*/
	perShard := (size  + dataShards + 4 - 1)/dataShards
    if perShard < 4 {
        perShard = 4
	}
	leftSpace := perShard * dataShards - size
	utils.AssertTrue(leftSpace >= 4)
	
	var padding []byte //include the last shard and all partyShards


	shards := dataShards + parityShards
	// calculate maximum number of full shards in `data` slice
	fullShards := len(input) / perShard
	if fullShards == 0 {
		//copy all data
		padding = make([]byte, shards * perShard)
		copy(padding, input)
		input= input[:0]
	} else {
		//copy the last shards
		padding = make([]byte, shards*perShard-perShard*fullShards)
		copy(padding, input[perShard*fullShards:])
		input = input[0 : perShard*fullShards]
	}
	// Split into equal-length shards.
	dst := make([][]byte, shards)
	i := 0
	for ; i < len(dst) && len(input) >= perShard; i++ {
		dst[i] = input[:perShard:perShard]
		input = input[perShard:]
	}

	for j := 0; i+j < len(dst); j++ {
		dst[i+j] = padding[:perShard:perShard]
		padding = padding[perShard:]
	}

	binary.BigEndian.PutUint32(dst[dataShards-1][perShard-4:], uint32(size))

	err = enc.Encode(dst)
	return dst, err

}


func which(x [][]byte) {
	for i := 0; i < len(x); i++ {
		if x[i] == nil {
			fmt.Printf("%d is zero\n", i)
		}
	}
}

func (ReedSolomon) RebuildECExtent(dataShards, parityShards int, sourceExtent []*extent.Extent, replacingIndex int, targetExtent *extent.Extent) error {
	
	targetExtent.AssertLock()

	enc, err := reedsolomon.New(dataShards, parityShards)
	
	blocks := make([][]*pb.Block, dataShards+parityShards)
	shards := make([][]byte, dataShards+parityShards)

	if len(sourceExtent) > replacingIndex  && sourceExtent[replacingIndex] != nil {
		return errors.New("sourceExtent[replacingIndex] must be nil")
	}

	var done bool
	start := uint32(0)
	end := uint32(0)
	n := 0
	for !done {
		for i:= 0 ; i < dataShards + parityShards ; i ++ {
			if sourceExtent[i] != nil {
				blocks[i], _, end, err = sourceExtent[i].ReadBlocks(start, 20, 40<<20)
				n = len(blocks[i])
				if err != nil {
					if err != wire_errors.EndOfExtent{
						return err
					}
					done= true
				}
			} else {
				blocks[i] = nil
			}
		}

		start = end
		writeBlocks := make([]*pb.Block, n)

		for k := 0; k < n; k++ {
			for i := 0 ; i < dataShards + parityShards ; i ++ {
				if blocks[i] == nil {
					shards[i] = nil
				} else {
					shards[i] = blocks[i][k].Data
				}
			}
			
			if err = enc.Reconstruct(shards) ; err != nil {
				return err
			}
			writeBlocks[k] = &pb.Block{
				Data: shards[replacingIndex],
			}
		}
		var doSync bool
		if done {//last one
			doSync = true
		}
		if _, _, err = targetExtent.AppendBlocks(writeBlocks, doSync); err != nil {
			return err
		}
	}

	return nil
}