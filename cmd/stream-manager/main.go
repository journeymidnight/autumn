package main

import (
	"github.com/journeymidnight/autumn/manager"
	"github.com/journeymidnight/autumn/manager/streammanager"
)

func main() {
	config := manager.NewConfig()
	etcd, client, err := manager.ServeETCD(config)
	if err != nil {
		panic(err.Error())
	}
	sm := streammanager.NewStreamManager(etcd, client, config)
	sm.ServeGRPC()
	sm.Close()

}
