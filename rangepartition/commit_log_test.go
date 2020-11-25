package rangepartition

import (
	"context"
	"fmt"
	"io"
	"testing"

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
	//already have the cluster
	sm := smclient.NewSMClient([]string{"127.0.0.1:3401"})
	err := sm.Connect()
	assert.Nil(t, err)

	commitLog, err := NewCommitLog(3, sm)

	assert.Nil(t, err)

	stopper := utils.NewStopper()

	for i := 0; i < 20; i++ {
		stopper.RunWorker(func() {
			ch := make(chan struct{})
			commitLog.Append(&pspb.LogEntry{
				Key:   "hello",
				Value: []byte("world"),
			}, func(entry *pspb.LogEntry, extendId uint64, offset uint32, innerOffset uint32, err error) {
				fmt.Printf("extentID: %d, offset: %d, innerOffset: %d, err :%v\n", extendId, offset, innerOffset, err)

				log, err := commitLog.Read(context.Background(), extendId, offset, innerOffset)
				assert.Nil(t, err)
				assert.Equal(t, entry, log)

				close(ch)
			})
			<-ch
		})
	}

	stopper.Wait()

	iter := commitLog.NewLogIter()
	//read out
	numOfLogs := 0
	for {
		entries, err := iter.Read()
		if err != nil && err != io.EOF {
			assert.Nil(t, err)
		}
		numOfLogs += len(entries)
		if err == io.EOF {
			break
		}
	}
	assert.Equal(t, 20, numOfLogs)
}
