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
	ID        uint64
	ListenURL string
	Dirs      []string
	WalDir    string
	SmAddr   []string
	EtcdAddr []string
}

func NewConfig() (*Config, error) {

	//https://github.com/urfave/cli/issues/599
	//cli does not support [Parsing int64, uint64, uint, time.Duration]
	//so we use the action function to receive all data

	var config Config
	var configFile string
	var dirSlice string
	var smAddrsSlice string
	var etcdAddrsSlice string

	flags := []cli.Flag{
		&cli.StringFlag{
			Name:        "config",
			Usage:       "Path to the config file",
			Aliases:     []string{"c"},
			Destination: &configFile,
		},
		&cli.StringFlag{
			Name:        "listenUrl",
			Usage:       "grpc listen url",
			Destination: &config.ListenURL,
		},
		&cli.StringFlag{
			Name:        "dirs",
			Usage:       "dirs",
			Destination: &dirSlice,
		},
		&cli.Uint64Flag{
			Name:        "ID",
			Destination: &config.ID,
		},
		&cli.StringFlag{
			Name:        "walDir",
			Destination: &config.WalDir,
		},
		&cli.StringFlag{
			Name:        "etcdAddrs",
			Destination: &etcdAddrsSlice,
		},
		(&cli.StringFlag{
			Name:        "smAddrs",
			Destination: &smAddrsSlice,
		}),
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
		config.SmAddr = utils.SplitAndTrim(smAddrsSlice, ",")
		config.EtcdAddr = utils.SplitAndTrim(etcdAddrsSlice, ",")
	}
	//validate config

	if err := validateConf(&config); err != nil {
		return nil, err
	}
	return &config, nil
}

func validateConf(conf *Config) error {
	if len(conf.ListenURL) == 0 {
		return errors.Errorf("listenUrl can not be empty")
	}

	if conf.ID == 0 {
		return errors.Errorf("ID can not be 0")
	}


	if len(conf.SmAddr) == 0 {
		return errors.Errorf("stream manager's address can not be null")
	}

	if len(conf.EtcdAddr) == 0 {
		return errors.Errorf("ETCD's address can not be null")
	}
	return nil
}
