package main

import (
    "flag"
    "log"
    "net/url"
    "os"
    "os/signal"
    "syscall"
    "time"

    "go.etcd.io/etcd/server/v3/embed"
)

func mustURL(raw string) url.URL {
    u, err := url.Parse(raw)
    if err != nil {
        log.Fatalf("parse url %s: %v", raw, err)
    }
    return *u
}

func main() {
    var name string
    var dir string
    var client string
    var peer string
    var cluster string

    flag.StringVar(&name, "name", "n1", "node name")
    flag.StringVar(&dir, "dir", "", "data dir")
    flag.StringVar(&client, "client", "http://127.0.0.1:2379", "listen+advertise client url")
    flag.StringVar(&peer, "peer", "http://127.0.0.1:2380", "listen+advertise peer url")
    flag.StringVar(&cluster, "cluster", "", "initial cluster")
    flag.Parse()

    if dir == "" {
        log.Fatal("dir is required")
    }
    if cluster == "" {
        cluster = name + "=" + peer
    }

    cfg := embed.NewConfig()
    cfg.Name = name
    cfg.Dir = dir
    cfg.LogLevel = "error"
    cfg.InitialCluster = cluster
    cfg.ClusterState = "new"

    cu := mustURL(client)
    pu := mustURL(peer)
    cfg.LCUrls = []url.URL{cu}
    cfg.ACUrls = []url.URL{cu}
    cfg.LPUrls = []url.URL{pu}
    cfg.APUrls = []url.URL{pu}

    e, err := embed.StartEtcd(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer e.Close()

    select {
    case <-e.Server.ReadyNotify():
        log.Printf("embedded etcd ready: %s", client)
    case <-time.After(30 * time.Second):
        e.Server.Stop()
        log.Fatal("embedded etcd not ready within timeout")
    }

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh
}
