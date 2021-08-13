package stream_manager

import (
	"testing"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/stretchr/testify/require"
)

func TestTaskPool(t *testing.T) {
	tp := NewTaskPool()
	tp.Insert(&pb.RecoveryTask{
		ExtentID: 10,
		ReplaceID: 3,
		NodeID: 2,
	})
	tp.Insert(&pb.RecoveryTask{
		ExtentID: 20,
		ReplaceID: 4,
		NodeID: 17,
	})
	tp.Insert(&pb.RecoveryTask{
		ExtentID: 30,
		ReplaceID: 1,
		NodeID: 17,
	})

	tasks := tp.GetFromNode(17)
	require.Equal(t, 2, len(tasks))


	require.Equal(t, true, tp.HasTask(10))
	require.Equal(t, false, tp.HasTask(100))


	rts := tp.GetFromNode(2)
	require.Equal(t, 1, len(rts))
	require.Equal(t, uint64(10), rts[0].ExtentID)


	rt := tp.GetFromExtent(10)
	require.NotNil(t, rt)

	tp.Remove(10)

	rt = tp.GetFromExtent(10)
	require.Nil(t, rt)
}