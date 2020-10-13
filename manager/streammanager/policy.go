package streammanager

import (
	"sort"

	"github.com/pkg/errors"
)

type AllocExtentPolicy interface {
	AllocExtent([]NodeStatus, int, []uint64) ([]NodeStatus, error)
}

type SimplePolicy struct{}

func (sp *SimplePolicy) AllocExtent(ns []NodeStatus, count int, keepNodes []uint64) ([]NodeStatus, error) {
	sort.Slice(ns, func(a, b int) bool {
		if ns[a].lastEcho.After(ns[b].lastEcho) {
			return true
		} else if ns[a].lastEcho.Before(ns[b].lastEcho) {
			return false
		}
		return ns[a].usage < ns[b].usage
	})

	set := make(map[uint64]bool)
	for _, id := range keepNodes {
		set[id] = true
	}

	var ret []NodeStatus
	for i := 0; i < count; i++ {
		if _, ok := set[ns[i].ID]; !ok {
			ret = append(ret, ns[i])
		}
	}
	if len(ret) < count {
		return nil, errors.Errorf("cannot find enough nodes")
	}
	return ret, nil
}
