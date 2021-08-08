package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/journeymidnight/autumn/etcd_utils"
)

func main() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: time.Second,
	})
	session, err := concurrency.NewSession(client, concurrency.WithTTL(30))

	client.Put(context.Background(), "stat", "stat")
	mutex := concurrency.NewMutex(session, "lock")
	err = mutex.Lock(context.Background())
	fmt.Printf("%+v", err)
	fmt.Printf("rev is %d\n", mutex.Header().Revision)

	ops := []clientv3.Op{
		clientv3.OpPut("hello", "world"),
	}

	err = etcd_utils.EtcdSetKVS(client, []clientv3.Cmp{
		mutex.IsOwner(),
	}, ops)

	fmt.Printf("%+v", err)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGUSR1)
	for {
		select {
		case <-sc:
			mutex.Unlock(context.Background())
			session.Close()
			return
		}
	}

}
