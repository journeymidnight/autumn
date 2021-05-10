package utils

import (
	"encoding/json"
	"io"

	"github.com/HdrHistogram/hdrhistogram-go"
)



type HistogramStatus struct {
	histogram *hdrhistogram.Histogram
}


func NewLantencyStatus(start int64 , end int64) *HistogramStatus{
	return &HistogramStatus{
		histogram: hdrhistogram.New(start, end, 3),
	}
}

func (ls *HistogramStatus) Record(n int64) error{
	return ls.histogram.RecordValue(n)
}

type Result struct {
	Percetage float64
	Lantency  float64
}

func (ls *HistogramStatus) Histgram(percentiles []float64, w io.Writer) []int64 {
	ret := make([]int64, len(percentiles))
	for i := range  percentiles {
		ret[i]  = ls.histogram.ValueAtQuantile(percentiles[i])
	}
	if w != nil {
		brackets := ls.histogram.CumulativeDistribution()
		results := make([]Result, len(brackets))
		for i := range brackets {
			results[i] = Result{
				Percetage:brackets[i].Quantile,
				Lantency: float64(brackets[i].ValueAt),
			}
		}
		data, _ := json.Marshal(results)
		w.Write(data)
	}
	return ret
}

/*
//https://blog.bramp.net/post/2018/01/16/measuring-percentile-latency/
type LantencyStatus struct {
	sortedBins []float64 //0-1, 2-3, 3-4, 4-5,
	binValues []int
}

func NewLantencyStatus(start float64, end float64, n int) *LantencyStatus {

	ls := &LantencyStatus{}
	step := (end - start)/float64(n)
	ls.binValues = make([]int, n)
	ls.sortedBins = make([]float64, n)
	bins := make(map[float64]int, n)
	for i := 0 ;i < n ; i ++ {
		ls.sortedBins[i]= start
		bins[start] = 0
		start +=step
	}

	return ls
}

func (ls *LantencyStatus) Record(n float64) error{
	index := sort.Search(len(ls.sortedBins), func(i int) bool {
		return ls.sortedBins[i] > n
	})
	if index == 0 {
		return errors.Errorf("out of range %f", n)
	}
	ls.binValues[index-1] ++
	return nil
}

func (ls *LantencyStatus) Histgram(p []float64, w io.Writer) []float64 {

	//runningTotal[i] = ls.binValues[i] + runningTotal[i - 1]
	n := len(ls.binValues)
	runningTotal := make([]int ,n)
	eCDF := make([]float64, n)
	runningTotal[0]=ls.binValues[0]
	for i := 1 ;i < n ;i ++ {
		runningTotal[i] = ls.binValues[i] + runningTotal[i-1]
	}
	for i := 0 ;i < n ;i ++ {
		eCDF[i] = float64(runningTotal[i]) / float64(runningTotal[n-1])
	}

	//p99: linear approximation
	ret := make([]float64, len(p))
	for i := range p {
		if p[i] > 1 || p[i] < 0 {
			panic(fmt.Sprintf("p should be in range [0-1]:%f", p[i]))
		}

		index := sort.Search(n, func(x int)bool {
			return eCDF[x] > p[i]
		})
		AssertTrue(index!=n)
		//index -1 , index
		if index == 0 {
			ret[i] = ls.sortedBins[0]
			continue
		}
		y1 := eCDF[index]
		y0 := eCDF[index-1]
		x0 := ls.sortedBins[index-1]
		x1 := ls.sortedBins[index]
		x := x0 + (x1-x0) * (p[i]-y0)/(y1-y0)
		ret[i] = x
	}

	if w != nil {
		pResult := make([]Result, n)
		//ignore the last
		for i := 0 ;i < n-1; i ++ {
			pResult[i].Lantency = ls.sortedBins[i+1]
			pResult[i].Percetage = eCDF[i]
		}
		//last v
		pResult[n-1].Percetage = 1
		pResult[n-1].Lantency = math.MaxFloat64
		data, err := json.Marshal(pResult)
		if err == nil {
			w.Write(data)
		}
	}


	return ret
}
*/

