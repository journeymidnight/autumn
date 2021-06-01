package erasure_code

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/klauspost/reedsolomon"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)


func init() {
	xlog.InitLog([]string{"test.log"}, zap.DebugLevel)
}


func TestLinear(t *testing.T) {
	enc, err := reedsolomon.New(2, 2)

	data1 := make([][]byte, 2 + 2)
	data1[0] = []byte("y")
	data1[1] = []byte("x")
	data1[2] = make([]byte,1)
	data1[3] = make([]byte,1)

	err = enc.Encode(data1)

	require.Nil(t, err)
	fmt.Printf("%+v\n", data1)

	data2 := make([][]byte, 2 + 2)
	data2[0] = []byte("12345")
	data2[1] = []byte("54321")
	data2[2] = make([]byte, 5)
	data2[3] = make([]byte, 5)

	err = enc.Encode(data2)
	fmt.Printf("%+v\n", data2)


	data3 := make([][]byte, 2+2)
	data3[0] = append(data1[0], data2[0]...)//[]byte("r12345")
	data3[1] = append(data1[1], data2[1]...)//[]byte("xasdfx")
	data3[2] = make([]byte, 6)
	data3[3] = make([]byte, 6)

	err = enc.Encode(data3)
	fmt.Printf("%+v\n", data3)
	require.Equal(t, data3[2], append(data1[2], data2[2]...))
	require.Equal(t, data3[3], append(data1[3], data2[3]...))

	
}


func TestRsCodecs(t *testing.T) {
	for _, size := range []int{8, 16, 4<<10, 6<<10, 8<<10, 100<<10, 1<<20, 8 << 20} {
		t.Run(fmt.Sprintf("size of data %d", size), func(t *testing.T){
			data := make([]byte, size)
			utils.SetRandStringBytes(data)
			//test 6+3
		
			output, err := ReedSolomon{}.Encode(data, 6, 3)
			require.Nil(t, err)
			require.Equal(t, 9,len(output))
			//fmt.Printf("the size of a shard is %d\n", len(output[0]))
		
			output[1] = nil
			output[5] = nil
			output[7] = nil
			//output[8] = nil
			haha, err := ReedSolomon{}.Decode(output, 6, 3)
			require.Nil(t, err)
			require.Equal(t,  len(data), len(haha))
			require.Equal(t, data, haha)

		})
	}
}


func TestReconstruct(t *testing.T) {

	size := 20 << 20//20M
	data := make([]byte, size)
	utils.SetRandStringBytes(data)
	//test 6+3
		
	output1, err := ReedSolomon{}.Encode(data, 6, 3)
	require.Nil(t, err)
	require.Equal(t, 9,len(output1))

	output2 , err := ReedSolomon{}.Encode(data, 6, 3)
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