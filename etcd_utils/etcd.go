package etcd_utils

import (
	"time"

	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func ServeETCD(cfg *embed.Config) (*embed.Etcd, *clientv3.Client, error) {

	utils.AssertTrue(xlog.Logger != nil)

	e, err := embed.StartEtcd(cfg)

	if err != nil {
		xlog.Logger.Fatal(err)
	}
	select {
	case <-e.Server.ReadyNotify():
		xlog.Logger.Info("Server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		xlog.Logger.Info("Server took too long to start!")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.ACUrls[0].String()},
		DialTimeout: time.Second,
	})

	return e, client, err

}
