package erasure_code

import (
	"io"
)

type ErasureCodec interface {
	Encode(input io.Reader, dataShards int, parityShards int, output []io.Writer, chunkSize int, fSize int64) error
	Reconstruct(input []io.Reader, dataShards int, parityShards int,  output []io.Writer, chunkSize int) error
}