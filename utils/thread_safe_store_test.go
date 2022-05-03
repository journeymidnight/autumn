package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestThreadSafeStore(t *testing.T) {
	// Test Add
	store := NewThreadSafeStore(Indexers{}, Indices{})
	store.Add("key1", "value1")
	item, ok := store.Get("key1")
	require.True(t, ok)
	require.Equal(t, "value1", item)
}


func TestThreadSafeIndex(t *testing.T) {
	type entry struct {
		key string
		loc int
		value int
	}
	
	keyFunc := func(obj interface{}) (string, error) {
		return obj.(*entry).key, nil
	}
	//index function for loc
	locIndexFunc := func(obj interface{}) ([]string, error) {
		entry, ok := obj.(*entry)
		if !ok {
			return nil, fmt.Errorf("unexpected type %T", obj)
		}
		return []string{fmt.Sprintf("%d",entry.loc)[:1]}, nil
	}
	index := NewIndexer(keyFunc, Indexers{"loc":locIndexFunc})

	for i := 0; i < 100; i++ {
		entry := &entry{fmt.Sprintf("key%d", i), i, i}
		index.Add(entry)
	}

	results , err := index.ByIndex("loc", "2")
	require.NoError(t, err)

	for _, obj := range results {
		pEntry, ok := obj.(*entry)
		require.True(t, ok)
		//value should start with 2
		firstChar := fmt.Sprintf("%d",pEntry.loc)[0]
		require.Equal(t, byte('2'), firstChar)
	}
}