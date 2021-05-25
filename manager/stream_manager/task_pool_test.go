package stream_manager

import (
	"testing"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/stretchr/testify/require"
)

func TestTaskPool(t *testing.T) {
	tp := NewTaskPool()
	tp.Insert(10, &pb.RecoveryTask{
		ExtentID: 10,
		ReplaceID: 3,
		NodeID: 2,
	})

	require.Equal(t, true, tp.HasTask(10))
	require.Equal(t, false, tp.HasTask(100))


	rt := tp.GetFromExtent(10)
	require.NotNil(t, rt)

	tp.Remove(10)

	rt = tp.GetFromExtent(10)
	require.Nil(t, rt)
}