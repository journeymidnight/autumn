package range_partition

import (
	"testing"

	"github.com/journeymidnight/autumn/range_partition/y"
	"github.com/stretchr/testify/require"
)

func TestEntry(t *testing.T) {
	value := []byte("hello world")
	key := []byte("key")

	entry := NewPutEntry(key, 0, uint32(len(value)))
	entry.WriteValue(value)
	err := entry.FinishWrite()

	require.NoError(t, err)
	require.Equal(t, key, y.ParseKey(entry.Key))
	require.Equal(t, value, entry.Value)

	data := entry.Encode()

	entry2, err := DecodeEntry(data)
	require.Nil(t, err)
	require.Equal(t, key, y.ParseKey(entry2.Key))
	require.Equal(t, value, entry2.Value)
}
