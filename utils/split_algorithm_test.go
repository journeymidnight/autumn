package utils

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileNameSplit(t *testing.T) {
	//FIXME: this test is not pass
	/*
	if n == 8
	[[0,7] [7,E] [E,L] [L,S] [S,Z] [Z,g] [g,n] [n,z]]
	if file name is zx, it can not find a region.

	if n == 100
    [[01,0c] [0c,1E] .... [y3,zy]]
	we have filename zyy, it can not find a region.
	and also we have a file name 00, it can not find a region
	*/
	algo := &FileNameSplitAlogrithm{}
	ret, err := algo.SplitAllRegions(100)
	require.Nil(t, err)
	require.Equal(t, 100, len(ret))
	//valid the start is last range's end
	for i := 1 ;i< len(ret) ; i ++ {
		require.Equal(t, ret[i-1].EndKey, ret[i].StartKey)
	}
}

func TestPrefixSplit(t *testing.T) {
	//FIXME: this test is not pass
	algo := NewPrefixSplitAlgorithm([][]byte{[]byte("a:"), []byte("b:")}, 8, &FileNameSplitAlogrithm{})
	ret, err := algo.SplitAllReginos(8)
	require.Nil(t, err)
	require.Equal(t, 8, len(ret))
	//valid the start is last range's end
	for i := 1 ;i< len(ret) ; i ++ {
		require.Equal(t, ret[i-1].EndKey, ret[i].StartKey)
	}

	for i := range ret {
		if !bytes.HasPrefix(ret[i].StartKey, []byte("a:")) && !bytes.HasPrefix(ret[i].StartKey, []byte("b:")){
			if i != 0 && i != len(ret)-1 {
				t.Errorf("%s is not a prefix", ret[i])
			}
		}
	}

}

func TestHexSplit(t *testing.T) {
	algo := NewHexStringSplitAlgorithm()
	n := 3
	ret, err := algo.SplitAllRegions(n)
	fmt.Printf("%v\n", ret)
	require.Nil(t, err)
	require.Equal(t, n, len(ret))
	require.Equal(t, "", string(ret[0].StartKey))
	require.Equal(t, "", string(ret[n-1].EndKey))

	//valid the start is last range's end
	for i := 1 ;i< len(ret) ; i ++ {
		require.Equal(t, ret[i-1].EndKey, ret[i].StartKey)
	}

	sort.Slice(ret, func(i, j int) bool {
		return bytes.Compare(ret[i].StartKey, ret[j].StartKey) < 0
	})

	//valid the start is last range's end
	for i := 1 ;i< len(ret) ; i ++ {
		require.Equal(t, ret[i-1].EndKey, ret[i].StartKey)
	}
}
