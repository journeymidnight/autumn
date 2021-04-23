package node

import (
	"os"

	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

type Config struct {
	ID uint64
	ListenUrl string
	Dirs []string
	WalDir string
}

func NewConfig() (*Config, error) {


	//https://github.com/urfave/cli/issues/599
	//cli does not support [Parsing int64, uint64, uint, time.Duration]
	//so we use the action function to receive all data

	var config Config
	var configFile string
	var dirSlice cli.StringSlice
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:    "config",
			Usage:   "Path to the config file",
			Aliases: []string{"c"},
			Destination: &configFile,
		},
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "listenUrl",
			Usage:       "grpc listen url",
			Destination: &config.ListenUrl,
		}),
		altsrc.NewStringSliceFlag(&cli.StringSliceFlag{
			Name:        "dirs",
			Usage:       "dirs",
			Destination: &dirSlice,
		}),
		altsrc.NewUint64Flag(&cli.Uint64Flag{
			Name:        "ID",
			Destination: &config.ID,
		}),
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "walDir",
			Destination: &config.WalDir,
		}),
		
	}
	app := &cli.App{
		Name: "extent-node",
		Flags: flags,
	}

	if err := app.Run(os.Args); err != nil {
		panic(err.Error())
	}


	_, err := os.Stat(configFile)
	if err == nil {
		_, err := toml.DecodeFile(configFile, &config)
		if err != nil {
			return nil, err
		}
	}
	//validate config

	if err := validateConf(&config) ; err != nil {
		return nil, err
	}

	return &config, nil
}

func validateConf(conf *Config) error{
	if len(conf.ListenUrl) ==  0 {
		return errors.Errorf("ListenUrl can not be empty")
	}

	if conf.ID == 0 {
		return errors.Errorf("ID can not be 0")
	}
	return nil
}