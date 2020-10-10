package manager

import (
	"log"
	"path/filepath"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/journeymidnight/autumn/xlog"

	"go.uber.org/zap/zapcore"
)

func ServeETCD(config *Config) (*embed.Etcd, *clientv3.Client, error) {
	cfg, err := config.GetEmbedConfig()
	if err != nil {
		xlog.Logger.Fatal(err)
	}
	e, err := embed.StartEtcd(cfg)

	xlog.InitLog([]string{filepath.Join(cfg.Dir, cfg.Name+".log")}, zapcore.DebugLevel)
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	select {
	case <-e.Server.ReadyNotify():
		xlog.Logger.Info("ETCD Server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		xlog.Logger.Info("Server took too long to start!")
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.ACUrls[0].String()},
		DialTimeout: time.Second,
	})
	return e, client, nil
}
