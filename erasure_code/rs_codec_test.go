package erasure_code

import (
	"bytes"
	"io"
	"testing"

	"github.com/journeymidnight/autumn/utils"
	"github.com/stretchr/testify/require"
)


func TestRsCodecs(t *testing.T) {
	data := make([]byte, 4 << 10)
	utils.SetRandStringBytes(data)
	//test 4+3
	input := bytes.NewBuffer(data)
	output := make([]io.Writer, 7)
	for i := range output {
		output[i] = new(bytes.Buffer)
	}
		
	//convert to io.Writer

	err := RSEncoder{}.Encode(input, 4, 3, output, 1<<10, 4 << 10)

	require.Nil(t, err)
	require.Equal(t,data[:1<<10], output[0].(*bytes.Buffer).Bytes())

	//reconstruct
	currupt := make([]io.Reader, len(output))
	for i := range currupt {
		if i == 0 || i == 3{
			currupt[i] = nil
			continue
		}
		currupt[i] = bytes.NewBuffer(output[i].(*bytes.Buffer).Bytes())
	}


	
	fixed := make([]io.Writer, 7)
	for i := range fixed {
		if i == 0 || i == 3{
			fixed[i] = new(bytes.Buffer)
			continue
		}
		fixed[i] = nil
	}

	err = RSEncoder{}.Reconstruct(currupt, 4, 3, fixed, 1 << 10, 1 << 10)
	require.Nil(t, err)
	require.Equal(t, data[:1<<10], fixed[0].(*bytes.Buffer).Bytes())
	require.Equal(t, data[3<<10:], fixed[3].(*bytes.Buffer).Bytes())

}
