package dlock

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/journeymidnight/autumn/utils"
)
type DLock struct {
	session *concurrency.Session
	name string
	mutex *concurrency.Mutex
}

var client *clientv3.Client


//stream manager reuse etcdclient
func InitEtcdClient (xclient *clientv3.Client) {
	client = xclient
}

//extent_node create a new etcd client
func InitDlocks(addrs []string) {
	var err error
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   addrs,
		DialTimeout: time.Second,
	})
	if err != nil {
		panic(err)
	}

}

func NewDLock(name string) *DLock {
	utils.AssertTrue(client != nil)
	session , err := concurrency.NewSession(client)
	if err != nil {
		panic(err)
	}
	mutex := concurrency.NewMutex(session, name)
	return &DLock{
		session: session,
		name:name,
		mutex: mutex,
	}
}


func (dl *DLock) Lock(to time.Duration) error {
	if to == 0 {
		return dl.mutex.Lock(context.Background())
	}
	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()
	return dl.mutex.Lock(ctx)
}

func (dl *DLock) Unlock() {
	dl.mutex.Unlock(context.Background())
	
}

func (dl *DLock) Close() {
	if dl.session != nil {
		dl.session.Close()
		dl.session = nil
	}

}
