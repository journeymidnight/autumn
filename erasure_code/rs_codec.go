package erasure_code

import (
	"encoding/binary"
	"io"

	"github.com/journeymidnight/autumn/utils"
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
	perShard := (size + dataShards - 1) / dataShards

	//at least 4 bytes to save length
	if perShard < 4 {
		perShard = 4
	}

	leftSpace := perShard * dataShards - size
	
	if leftSpace < 4 {
		perShard += (4-leftSpace) / dataShards + 1
	}

	leftSpace = perShard * dataShards - size
	
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

