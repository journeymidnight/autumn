package erasure_code

import (
	"io"
)

type ErasureCodec interface {
	Encode(input []byte, dataShards int, parityShards int, chunkSize int) ([][]byte, error)
	Decode(input [][]byte, dataShards uint32, parityShards uint32, chunkSize int) ([]byte, error)
	Reconstruct(input []io.Reader, dataShards int, parityShards int, output []io.Writer, chunkSize int) error
}
