package manager

import (
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

func ServeETCD(config *Config) (*embed.Etcd, *clientv3.Client, error) {

	utils.AssertTrue(xlog.Logger != nil)

	cfg, err := config.GetEmbedConfig()
	if err != nil {
		xlog.Logger.Fatal(err)
	}
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
