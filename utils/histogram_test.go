package utils

import (
	"fmt"
	"os"
	"testing"
)

func TestLantency(t *testing.T) {
	ls := NewLantencyStatus(0, 1000)
	
	for i := 0 ;i < 100 ; i ++ {
		ls.Record(10)
	}
	ls.Record(800)
	ret := ls.Histgram([]float64{50, 95, 99}, os.Stdout)
	fmt.Printf("\n\n%+v\n\n\n", ret)
}