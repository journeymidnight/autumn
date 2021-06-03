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
}

type OptionFunc func(*Option)

func DefaultOption() OptionFunc {
	return func(opt *Option) {
		opt.MaxSkipList = 64 * MB
		opt.WriteChCapacity = 64
	}
}

func TestOption() OptionFunc {
	return func(opt *Option) {
		opt.MaxSkipList = 1 * MB
		opt.WriteChCapacity = 16
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
