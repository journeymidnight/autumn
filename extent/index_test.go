package extent

import (
	"testing"

	"github.com/journeymidnight/streamlayer/proto/pb"
	"github.com/stretchr/testify/assert"
)

func TestStaticIndex(t *testing.T) {
	cases := []pb.BlockMeta{
		{
			BlockLength: 512,
			BlockOffset: 1024,
			Offset:      1024,
		},

		{
			BlockLength: 512,
			BlockOffset: 2048,
			Offset:      2048,
		},

		{
			BlockLength: 4096,
			BlockOffset: 2048,
			Offset:      4096,
		},
	}

	index := NewDynamicIndex()
	for i := range cases {
		index.Put(cases[i].Offset, &cases[i])
	}
	f := newMemory(8192)
	err := index.Marshal(f)
	assert.Nil(t, err)

	f.resetPos()
	staticIndex := NewStaticIndex()
	err = staticIndex.Unmarshal(f)
	assert.Nil(t, err)

	assert.Equal(t, cases, staticIndex.x)
}

func TestDynamicIndex(t *testing.T) {
	index := NewDynamicIndex()
	//all cases should be sorted by Offset
	cases := []pb.BlockMeta{
		{
			BlockLength: 512,
			BlockOffset: 1024,
			Offset:      1024,
		},

		{
			BlockLength: 512,
			BlockOffset: 2048,
			Offset:      2048,
		},

		{
			BlockLength: 4096,
			BlockOffset: 2048,
			Offset:      4096,
		},
	}

	//test index put and set
	for i := range cases {
		index.Put(cases[i].Offset, &cases[i])
	}

	for i := range cases {
		r, ok := index.Get(cases[i].Offset)
		assert.True(t, ok)
		assert.Equal(t, cases[i], *r)
	}

	f := newMemory(8192)
	err := index.Marshal(f)
	assert.Nil(t, err)

	newIndex := NewDynamicIndex()
	f.resetPos()

	err = newIndex.Unmarshal(f)
	assert.Nil(t, err)

}
