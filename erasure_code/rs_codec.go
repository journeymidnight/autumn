package erasure_code

import (
	"bytes"
	"io"
	"math"

	"github.com/journeymidnight/autumn/utils"
	"github.com/klauspost/reedsolomon"
	"github.com/pkg/errors"
)

type RSEncoder struct{} 

func (RSEncoder ) Reconstruct(input []io.Reader, dataShards int, parityShards int,  output []io.Writer, chunkSize int, fSize int64) error{
	enc, err := reedsolomon.NewStream(dataShards, parityShards)
	if err != nil {
		return err
	}
	for i := int64(0); i < fSize ; i += int64(chunkSize) {
		in := make([]io.Reader, dataShards + parityShards)
		for i := range in {
			if input[i] == nil {
				in[i] = nil
			} else {
				in[i] = io.LimitReader(input[i], int64(chunkSize))
			}
		}
	
		if err = enc.Reconstruct(in, output) ; err != nil {
			return err
		}
		
	}
	return nil
}


type Cached struct {
	io.ReadWriter
	wb *bytes.Buffer
	rb *bytes.Buffer
}

func NewCached() *Cached {
	return &Cached{
		wb: new(bytes.Buffer),
		rb:nil,
	}
}
func (c *Cached) Write(p []byte) (n int, err error) {
	return c.wb.Write(p)
}

func (c *Cached) ResetForRead() {
	c.rb = bytes.NewBuffer(c.wb.Bytes())
	return
}

func (c *Cached) Read(p []byte) (n int, err error) {
	return c.rb.Read(p)
}



func (RSEncoder) Encode(input io.Reader, dataShards int, parityShards int, output []io.Writer, chunkSize int, fSize int64) error {
	if len(output) != dataShards + parityShards {
		return errors.Errorf("%d+%d != len(output) [%d]", dataShards, parityShards, len(output))
	}
	
	enc, err := reedsolomon.NewStream(dataShards, parityShards)
	if err != nil {
		return err
	}
	groupSize := int64(dataShards * chunkSize)


	utils.AssertTrue(fSize <= math.MaxUint32)
	utils.AssertTrue(chunkSize & (chunkSize - 1)==0)


	for i := int64(0); i < fSize ; i+= int64(groupSize) {

		r := io.LimitReader(input, int64(groupSize))

		cached := make([]*Cached, dataShards, dataShards)
		
		for i := range cached {
			cached[i] = NewCached() 
		}

		multiWrites := make([]io.Writer, dataShards, dataShards)
		for i := range output[:dataShards] {
			multiWrites[i] = io.MultiWriter(cached[i], output[i])
		}
		enc.Split(r, multiWrites, groupSize) //write dataShards


		for i := range cached {
			cached[i].ResetForRead()
		}

		
		rCache := make([]io.Reader, len(cached))
		for i := range rCache {
			rCache[i] = cached[i]
		}
		err = enc.Encode(rCache, output[dataShards:])
		if err != nil {
			return err
		}
	}
	return nil
}
