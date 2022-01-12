package node

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/journeymidnight/autumn/utils"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

type Config struct {
	ID           uint64
	ListenURL    string
	Dirs         []string
	WalDir       string
	SmURLs       []string
	EtcdURLs     []string
	TraceSampler float64
}

func NewConfig() (*Config, error) {

	//https://github.com/urfave/cli/issues/599
	//cli does not support [Parsing int64, uint64, uint, time.Duration]
	//so we use the action function to receive all data

	var config Config
	var configFile string
	var dirSlice string
	var smURLsSlice string
	var etcdURLsSlice string

	flags := []cli.Flag{
		&cli.StringFlag{
			Name:        "config",
			Usage:       "Path to the config file",
			Aliases:     []string{"c"},
			Destination: &configFile,
		},
		&cli.StringFlag{
			Name:        "listen-url",
			Usage:       "grpc listen url",
			Destination: &config.ListenURL,
		},
		&cli.StringFlag{
			Name:        "dirs",
			Usage:       "dirs",
			Destination: &dirSlice,
		},
		&cli.Uint64Flag{
			Name:        "id",
			Destination: &config.ID,
		},
		&cli.StringFlag{
			Name:        "waldir",
			Destination: &config.WalDir,
		},
		&cli.StringFlag{
			Name:        "etcd-urls",
			Destination: &etcdURLsSlice,
		},
		&cli.StringFlag{
			Name:        "sm-urls",
			Destination: &smURLsSlice,
		},
		&cli.Float64Flag{
			Name:        "trace-sampler",
			Destination: &config.TraceSampler,
		},
	}

	app := &cli.App{
		Name:  "extent-node",
		Flags: flags,
	}

	if err := app.Run(os.Args); err != nil {
		panic(err.Error())
	}

	if len(configFile) > 0 {
		if _, err := os.Stat(configFile); err != nil {
			return nil, err
		}
		//if we have config file, override all data
		_, err := toml.DecodeFile(configFile, &config)
		if err != nil {
			return nil, err
		}
		fmt.Printf("node %d reading config file %s\n", config.ID, configFile)
	} else {
		config.Dirs = utils.SplitAndTrim(dirSlice, ",")
		config.SmURLs = utils.SplitAndTrim(smURLsSlice, ",")
		config.EtcdURLs = utils.SplitAndTrim(etcdURLsSlice, ",")
	}
	//validate config

	if err := validateConf(&config); err != nil {
		return nil, err
	}
	return &config, nil
}

func validateConf(conf *Config) error {
	if len(conf.ListenURL) == 0 {
		return errors.Errorf("--listen-url can not be empty")
	}

	if conf.ID == 0 {
		return errors.Errorf("--id can not be 0")
	}

	if len(conf.Dirs) == 0 {
		return errors.Errorf("--dir can not be null")
	}

	if len(conf.SmURLs) == 0 {
		return errors.Errorf("stream manager's urls  can not be null")
	}

	if len(conf.EtcdURLs) == 0 {
		return errors.Errorf("ETCD's urls can not be null")
	}
	return nil
}
