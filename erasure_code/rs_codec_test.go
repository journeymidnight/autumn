package erasure_code

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)


func init() {
	xlog.InitLog([]string{"test.log"}, zap.DebugLevel)
}

func TestRsCodecs(t *testing.T) {
	for _, size := range []int{8, 16, 4<<10, 6<<10, 8<<10, 100<<10, 1<<20} {
		t.Run(fmt.Sprintf("size of data %d", size), func(t *testing.T){
			data := make([]byte, size)
			utils.SetRandStringBytes(data)
			//test 6+3
		
			output, err := ReedSolomon{}.Encode(data, 6, 3, 4<<10)
			require.Nil(t, err)
			require.Equal(t, 9,len(output))
		
			output[0] = nil
			output[5] = nil
			output[7] = nil
			//output[8] = nil
			haha, err := ReedSolomon{}.Decode(output, 6, 3, 4 << 10)
			require.Nil(t, err)
			require.Equal(t,  len(data), len(haha))
			require.Equal(t, data, haha)

		})
	}
}


func TestReconstruct(t *testing.T) {

	size := 1 << 20//1M
	data := make([]byte, size)
	utils.SetRandStringBytes(data)
	//test 6+3
		
	output1, err := ReedSolomon{}.Encode(data, 6, 3, 4<<10)
	require.Nil(t, err)
	require.Equal(t, 9,len(output1))

	output2 , err := ReedSolomon{}.Encode(data, 6, 3, 4<<10)
	require.Nil(t, err)
	require.Equal(t, 9,len(output2))

	var output [][]byte = make([][]byte, 9)
	for i := 0 ;i < 9 ; i ++ {
		output[i] = append(output1[i], output2[i]...)
	}


	reConstructInput := make([]io.Reader, 9)
	for i := 0 ; i < 9 ; i ++ {
		reConstructInput[i] = bytes.NewBuffer(output[i])
	}
	reConstructInput[3] = nil //3 is missing
	reConstructInput[0] = nil //0 is missing
	reConstructInput[2] = nil //0 is missing




	var reConstructOutput [9]io.Writer
	reConstructOutput[3] = new(bytes.Buffer)
	

	err = ReedSolomon{}.Reconstruct(reConstructInput, 6, 3, reConstructOutput[:])
	require.Nil(t, err)
	x := reConstructOutput[3].(*bytes.Buffer).Bytes()
	require.Equal(t, output[3], x)


}