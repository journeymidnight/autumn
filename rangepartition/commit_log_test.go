package rangepartition

import (
	"fmt"
	"testing"
	"time"

	"github.com/journeymidnight/autumn/manager/smclient"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func init() {
	xlog.InitLog([]string{"test.log"}, zapcore.DebugLevel)
}

func TestCommitLog(t *testing.T) {
	//assume have cluster
	sm := smclient.NewSMClient([]string{"127.0.0.1:3401"})
	err := sm.Connect()
	assert.Nil(t, err)

	commitLog := NewCommitLog(63, sm)

	stopper := utils.NewStopper()

	for i := 0; i < 20; i++ {
		stopper.RunWorker(func() {
			ch := make(chan struct{})
			commitLog.Append(&pspb.LogEntry{
				Key:   "hello",
				Value: []byte("world"),
			}, func(entry *pspb.LogEntry, extendId uint64, offset uint32, innerOffset int) {
				fmt.Printf("extentID: %d, offset: %d, innerOffset: %d\n", extendId, offset, innerOffset)
				close(ch)
			})
			<-ch

		})

	}
	time.Sleep(time.Second)
	stopper.Wait()
}
