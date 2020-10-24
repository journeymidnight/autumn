package utils

import (
	"fmt"
	"hash"
	"hash/adler32"
	"math"
	"math/rand"
	"strings"
	"sync"

	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
)

var (
	hashPool = sync.Pool{
		New: func() interface{} {
			return adler32.New()
		},
	}
)

func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// AssertTrue asserts that b is true. Otherwise, it would log fatal.
func AssertTrue(b bool) {
	if !b {
		panic(fmt.Sprintf("%+v", errors.Errorf("Assert failed")))
	}
}

func EqualUint32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func SetRandStringBytes(data []byte) {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	for i := range data {
		data[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
}

func AdlerCheckSum(data []byte) uint32 {
	hash := hashPool.Get().(hash.Hash32)
	defer hashPool.Put(hash)
	hash.Reset()
	hash.Write(data)
	return hash.Sum32()
}

func Check(err error) {
	if err != nil {
		xlog.Logger.Fatalf("%+v", errors.Wrap(err, ""))
	}
}

func HumanReadableThroughput(t float64) string {
	if t < 0 || t < 1e-9 { //if t <=0 , return ""
		return ""
	}
	units := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	power := int(math.Log10(t) / 3)
	if power >= len(units) {
		return ""
	}

	return fmt.Sprintf("%.2f%s/sec", t/math.Pow(1000, float64(power)), units[power])
}

func SplitAndTrim(s string, sep string) []string {
	parts := strings.Split(s, sep)
	for i := 0; i < len(parts); i++ {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
