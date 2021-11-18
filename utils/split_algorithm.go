package utils

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/pkg/errors"
)

type SplitAlgorithm interface {
	Split(start, end []byte, numSplits int) ([]Range, error)
	SplitAllReginos(numSplits int) ([]Range, error)
}

type FileNameSplitAlogrithm struct {
	space  []byte
	result [][]byte
}

func (a *FileNameSplitAlogrithm) Split(start, end []byte, numSplits int) ([]Range, error) {
	panic("implement me")
}

type Range struct {
	start []byte
	end   []byte
}

func (r Range) String() string {
	return fmt.Sprintf("[%s,%s]", r.start, r.end)
}

func (a *FileNameSplitAlogrithm) SplitAllReginos(numSplits int) ([]Range, error) {
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
				start: a.result[j],
				end:   a.result[j+range_],
			})
		}
		j += range_
	}
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

type UniformSplitAlgorithm struct {
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
	surfixRanges, err := algo.endfix.SplitAllReginos(n)
	if err != nil {
		return nil, err
	}
	var ret []Range
	for i := range algo.prefix {
		for j := range surfixRanges {
			ret = append(ret, Range{
				start: append(algo.prefix[i], surfixRanges[j].start...),
				end:   append(algo.prefix[i], surfixRanges[j].end...),
			})
		}
	}
	return ret, nil
}
