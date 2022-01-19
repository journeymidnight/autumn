package range_partition

import (
	"github.com/journeymidnight/autumn/range_partition/table"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

/*
1 * MB for test
60 * MB for production
maxSkipList = 1 * MB



64 for production
16 for test
writeChCapacity = 64

*/

type Option struct {
	MaxSkipList     int64 //max size of skiplist, when compacting, the max is 2 * MaxSkipList
	WriteChCapacity int   //write channel length
	MustSync        bool
	MaxExtentSize   uint32
	CompressionType table.CompressionType
}

type OptionFunc func(*Option)

func DefaultOption() OptionFunc {
	return func(opt *Option) {
		opt.MaxSkipList = 64 * MB
		opt.WriteChCapacity = 64
		opt.MustSync = true
		opt.MaxExtentSize = 1 * GB
		opt.CompressionType = table.Snappy
	}
}

func TestOption() OptionFunc {
	return func(opt *Option) {
		opt.MaxSkipList = 1 * MB
		opt.WriteChCapacity = 16
		opt.MustSync = false
		opt.MaxExtentSize = 8 * MB
		opt.CompressionType = table.None
	}
}

func WithSync(b bool) OptionFunc {
	return func(opt *Option) {
		opt.MustSync = b
	}
}
func WithMaxSkipList(n int64) OptionFunc {
	return func(opt *Option) {
		opt.MaxSkipList = n
	}
}
func WriteChCapacity(n int) OptionFunc {
	return func(opt *Option) {
		opt.WriteChCapacity = n
	}
}

func WithCompression(codec string) OptionFunc {
	return func(opt *Option) {
		switch codec {
		case "snappy":
			opt.CompressionType = table.Snappy
		case "zstd":
			opt.CompressionType = table.ZSTD
		case "none":
			opt.CompressionType = table.None
		default:
			xlog.Logger.Fatal("unknown compression type:", codec)
		}
	}
}

func MaxExtentSize(n uint32) OptionFunc {
	utils.AssertTruef(n < (3<<30), "MaxExtentSize must less than 3GB")
	return func(opt *Option) {
		opt.MaxExtentSize = n
	}
}
