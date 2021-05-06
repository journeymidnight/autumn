package erasure_code

import (
	"fmt"
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
			//output[7] = nil
			//output[8] = nil
			haha, err := ReedSolomon{}.Decode(output, 6, 3, 4 << 10)
			require.Nil(t, err)
			require.Equal(t,  len(data), len(haha))
			require.Equal(t, data, haha)

		})
	}

}
