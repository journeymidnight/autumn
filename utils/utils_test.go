package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlign(t *testing.T) {
	require.Equal(t, uint32(0), Ceil(0, 4))
	require.Equal(t, uint32(4), Ceil(1, 4))
	require.Equal(t, uint32(4), Ceil(2, 4))
	require.Equal(t, uint32(4), Ceil(3, 4))
	require.Equal(t, uint32(4), Ceil(4, 4))


	require.Equal(t, uint32(0), Floor(2, 8))
	require.Equal(t, uint32(0), Floor(3, 8))
	require.Equal(t, uint32(0), Floor(4, 8))
	require.Equal(t, uint32(8), Floor(9, 8))
	require.Equal(t, uint32(8), Floor(8, 8))





}

