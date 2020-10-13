package utils

import (
	"fmt"
	"hash"
	"hash/adler32"
	"math/rand"
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
