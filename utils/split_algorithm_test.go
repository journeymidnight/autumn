package utils

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileNameSplit(t *testing.T) {
	algo := &FileNameSplitAlogrithm{}
	ret, err := algo.SplitAllReginos(100)
	require.Nil(t, err)
	require.Equal(t, 100, len(ret))
}

func TestPrefixSplit(t *testing.T) {
	algo := NewPrefixSplitAlgorithm([][]byte{[]byte("a:"), []byte("b:")}, 8, &FileNameSplitAlogrithm{})
	ret, err := algo.SplitAllReginos(8)
	require.Nil(t, err)
	require.Equal(t, 8, len(ret))

	for i := range ret {
		if !bytes.HasPrefix(ret[i].start, []byte("a:")) && !bytes.HasPrefix(ret[i].start, []byte("b:")) {
			t.Errorf("%s is not a prefix", ret[i])
		}
	}

}
