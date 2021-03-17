package manager

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/coreos/etcd/embed"
	"github.com/urfave/cli/v2"
)

type Config struct {
	Name                string
	Dir                 string
	ClientUrls          string // --listen-client-urls
	PeerUrls            string // --listen-peer-urls
	AdvertisePeerUrls   string // --advertise-peer-urls
	AdvertiseClientUrls string // --advertise-client-urls
	InitialCluster      string // --initial-cluster
	InitialClusterState string // --initial-cluster-state
	ClusterToken        string

	GrpcUrl string // --listen-stream-manager-grpc
	//GrpcUrlPM string
}

func parseUrls(s string) (ret []url.URL, err error) {
	for _, urlString := range strings.Split(s, ",") {
		u, err := url.Parse(urlString)
		if err != nil {
			return nil, err
		}
		ret = append(ret, *u)
	}
	return ret, nil
}

func (config *Config) GetEmbedConfig() (*embed.Config, error) {
	embedConfig := embed.NewConfig()
	embedConfig.Name = config.Name
	embedConfig.Dir = config.Dir
	embedConfig.ClusterState = config.InitialClusterState
	embedConfig.InitialClusterToken = config.ClusterToken
	embedConfig.InitialCluster = config.InitialCluster

	lcurls, err := parseUrls(config.ClientUrls)
	if err != nil {
		return nil, err
	}
	embedConfig.LCUrls = lcurls

	acurls, err := parseUrls(config.AdvertiseClientUrls)
	if err != nil {
		return nil, err
	}
	embedConfig.ACUrls = acurls

	lpurls, err := parseUrls(config.PeerUrls)
	if err != nil {
		return nil, err
	}
	embedConfig.LPUrls = lpurls

	apurls, err := parseUrls(config.AdvertisePeerUrls)
	if err != nil {
		return nil, err
	}
	embedConfig.APUrls = apurls

	return embedConfig, nil
}

func NewConfig() *Config {
	var config Config
	var err error
	app := &cli.App{
		HelpName: "zero",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "dir",
				Usage:       "dir for etcd",
				Destination: &config.Dir,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "name",
				Usage:       "human-readable name for etcd",
				Destination: &config.Name,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "listen-client-urls",
				Usage:       "client url listen on",
				Destination: &config.ClientUrls,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "advertise-client-urls",
				Usage:       "advertise url for client traffic",
				Destination: &config.AdvertiseClientUrls,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "listen-peer-urls",
				Usage:       "",
				Destination: &config.PeerUrls,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "advertise-peer-urls",
				Usage:       "",
				Destination: &config.AdvertisePeerUrls,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "initial-cluster",
				Usage:       "initial cluster configuration for bootstrapping",
				Destination: &config.InitialCluster,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "initial-cluster-state",
				Usage:       "",
				Destination: &config.InitialClusterState,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "initial-cluster-token",
				Usage:       "",
				Destination: &config.ClusterToken,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "listen-grpc",
				Destination: &config.GrpcUrl,
				Required:    true,
			},
			/*
				&cli.StringFlag{
					Name:        "listen-grpc-pm",
					Destination: &config.GrpcUrlPM,
					Required:    true,
				},
			*/
		},
	}
	if err = app.Run(os.Args); err != nil {
		fmt.Println(app.Usage)
		panic(fmt.Sprintf("%v", err))
	}
	//xlog.Logger.Infof("%+v", config)
	return &config
}
