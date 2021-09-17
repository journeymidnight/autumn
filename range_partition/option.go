package range_partition

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
	MustSync bool 
	TruncateSize uint32
}

type OptionFunc func(*Option)

func DefaultOption() OptionFunc {
	return func(opt *Option) {
		opt.MaxSkipList = 64 * MB
		opt.WriteChCapacity = 64
		opt.MustSync = true
		opt.TruncateSize = 1 * GB
	}
}

func TestOption() OptionFunc {
	return func(opt *Option) {
		opt.MaxSkipList = 1 * MB
		opt.WriteChCapacity = 16
		opt.MustSync = true
		opt.TruncateSize = 8 * MB
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

func TruncateSize(n uint32) OptionFunc {
	return func(opt *Option) {
		opt.TruncateSize = n
	}
}