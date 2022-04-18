package utils

import (
	"bytes"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

type SplitAlgorithm interface {
	Split(start, end []byte, numSplits int) ([]Range, error)
	SplitAllRegions(numSplits int) ([]Range, error)
}

type FileNameSplitAlogrithm struct {
	space  []byte
	result [][]byte
}

func (a *FileNameSplitAlogrithm) Split(start, end []byte, numSplits int) ([]Range, error) {
	panic("implement me")
}

type Range struct {
	StartKey []byte
	EndKey   []byte
}

func (r Range) String() string {
	return fmt.Sprintf("[%s,%s]", r.StartKey, r.EndKey)
}

func (a *FileNameSplitAlogrithm) SplitAllRegions(numSplits int) ([]Range, error) {
	a.space = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
	iters, range_, err := a.possibleRegions(numSplits)
	if err != nil {
		return nil, err
	}
	//find all permutations for space and iters
	a.permutations(0, iters)

	sort.Slice(a.result, func(i, j int) bool {
		return bytes.Compare(a.result[i], a.result[j]) < 0
	})

	var ret []Range
	j := 0
	for i := 0; i < numSplits; i++ {
		if i == numSplits-1 {
			ret = append(ret, Range{a.result[j], a.result[len(a.result)-1]})
		} else {
			ret = append(ret, Range{
				StartKey: a.result[j],
				EndKey:   a.result[j+range_],
			})
		}
		j += range_
	}
	//Fix ret, make sure the start key is "", and the end key is ""
	ret[0].StartKey = []byte("")
	ret[len(ret)-1].EndKey = []byte("")
	return ret, nil
}

func (a *FileNameSplitAlogrithm) permutations(first int, iters int) {
	if first == iters {
		var tmp []byte
		tmp = append(tmp, a.space[:first]...)
		a.result = append(a.result, tmp)
		return
	}
	for i := first; i < len(a.space); i++ {
		a.space[first], a.space[i] = a.space[i], a.space[first]
		a.permutations(first+1, iters)
		a.space[first], a.space[i] = a.space[i], a.space[first]
	}
}

// possibleRegions returns (iters, range, error)
func (a *FileNameSplitAlogrithm) possibleRegions(n int) (int, int, error) {
	sum := 1
	for i := 1; i < 10; i++ {
		sum *= len(a.space) - (i - 1)
		if sum >= n {
			return i, sum / n, nil
		}
	}
	return 0, 0, errors.New("numSplit is too big")
}

type PrefixSplitAlgorithm struct {
	prefix [][]byte
	endfix SplitAlgorithm
}

func NewPrefixSplitAlgorithm(prefixs [][]byte, numSplits int, endfix SplitAlgorithm) *PrefixSplitAlgorithm {

	if len(prefixs) == 0 {
		panic("invalid prefixs")
	}

	if numSplits/len(prefixs) < 2 {
		panic("numSplits is too small")
	}

	return &PrefixSplitAlgorithm{
		prefix: prefixs,
		endfix: endfix,
	}
}

func (algo *PrefixSplitAlgorithm) Split(start, end []byte, numSplits int) ([]Range, error) {
	panic("implement me")
}

func (algo *PrefixSplitAlgorithm) SplitAllReginos(numSplits int) ([]Range, error) {
	n := numSplits / len(algo.prefix)
	surfixRanges, err := algo.endfix.SplitAllRegions(n)
	if err != nil {
		return nil, err
	}
	var ret []Range
	for i := range algo.prefix {
		for j := range surfixRanges {
			ret = append(ret, Range{
				StartKey: append(algo.prefix[i], surfixRanges[j].StartKey...),
				EndKey:   append(algo.prefix[i], surfixRanges[j].EndKey...),
			})
		}
	}
	//output is [[a:,a:F] [a:F,a:U] [a:U,a:j] [a:j,a:] [b:,b:F] [b:F,b:U] [b:U,b:j] [b:j,b:]]
	//fix this output to [[,a:F] [a:F,a:U] [a:U,a:j] [a:j,b:] [b:,b:F] [b:F,b:U] [b:U,b:j] [b:j,]]
	for i := 1 ;i< len(ret) ; i ++ {
		if !bytes.Equal(ret[i].StartKey, ret[i-1].EndKey){
			ret[i-1].EndKey = ret[i].StartKey
		}
	}
	ret[len(ret)-1].EndKey = []byte("")
	ret[0].StartKey = []byte("")
	return ret, nil
}


type HexStringSplitAlgorithm struct {}

func NewHexStringSplitAlgorithm() *HexStringSplitAlgorithm {
	return &HexStringSplitAlgorithm{}
}

//split's code is from https://github.com/apache/hbase/blob/35aa57e4452c6f0a7f5037371edca64163913345/hbase-server/src/main/java/org/apache/hadoop/hbase/util/RegionSplitter.java
//start and end must be hex string
func (algo *HexStringSplitAlgorithm) Split(start, end []byte, numSplits int) ([]Range, error) {

	if !validHexByte(start) || !validHexByte(end) {
		return nil, errors.Errorf("invalid start or end: %s, %s", start, end)
	}

	rowCompareLength := len(end)

	startInt := new(big.Int)
	endInt := new(big.Int)

	startInt.SetString(string(start), 16)
	endInt.SetString(string(end), 16)

	if startInt.Cmp(endInt) > 0 {
		return nil, errors.Errorf("start %s is bigger than end %s", start, end)
	}
	if numSplits < 2 {
		return nil, errors.Errorf("numSplits %d is too small", numSplits)
	}
	rangeInt :=new(big.Int).Sub(endInt, startInt)
	sizeOfEachSplit := rangeInt.Div(rangeInt, big.NewInt(int64(numSplits)))

	splits := make([]*big.Int, numSplits - 1)
	for i := 0 ; i < numSplits - 1; i ++ {
		x := new(big.Int).Mul(sizeOfEachSplit, big.NewInt(int64(i+1)))
		splits[i] = new(big.Int).Add(startInt, x)
	}

	rangeStart := startInt
	ranges := make([]Range, numSplits)
	//fill the first numSplits - 1 ranges
	for i := 0; i < numSplits - 1 ; i++ {
		ranges[i] = Range{
			StartKey: convertToHex(rangeStart,rowCompareLength),
			EndKey:  convertToHex(splits[i], rowCompareLength),
		}
		rangeStart = splits[i]
	}
	//fix the first range, we must start from ""
	ranges[0].StartKey = []byte("")
	//fill the last range, we must end with ""
	ranges[len(ranges)-1] = Range{
		StartKey: convertToHex(splits[len(splits)-1], rowCompareLength),
		EndKey: []byte(""),
	}

	return ranges, nil
}

func (algo *HexStringSplitAlgorithm) SplitAllRegions(numSplits int) ([]Range, error) {
	return algo.Split([]byte("00000000"), []byte("FFFFFFFF"), numSplits)
}

func convertToHex(s *big.Int, pad int) []byte {
	str := s.Text(16)
	//left padding
	if len(str) < pad {
		str = strings.Repeat("0", pad - len(str)) + str
	}
	return []byte(str)
}

func isHex(s byte) bool {
	return (s >= '0' && s <= '9') || (s >= 'a' && s <= 'f') || (s >= 'A' && s <= 'F')
}
func validHexByte(s []byte) bool {
	for i := 0; i < len(s); i++ {
		if !isHex(s[i]) {
			return false
		}
	}
	return true
}