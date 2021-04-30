package erasure_code

import (
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
	data := make([]byte, 4 << 10)
	utils.SetRandStringBytes(data)
	//test 6+3

	output, err := RSEncoder{}.Encode(data, 6, 3, 4<<10)
	require.Nil(t, err)
	require.Equal(t, 9,len(output))

	output[0] = nil
	output[5] = nil
	output[8] = nil
	haha, err := RSEncoder{}.Decode(output, 6, 3, 4 << 10)
	require.Nil(t, err)
	require.Equal(t,  len(data), len(haha))
	require.Equal(t, data, haha)

}
