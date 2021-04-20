package node

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

type Config struct {
	ID uint64
	ListenUrl string
	Dirs []string
	WalDir string
}

func NewConfig() *Config {
	var config Config
	var slices cli.StringSlice
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:    "config",
			Usage:   "Path to the config file",
		},
		altsrc.NewStringFlag(&cli.StringFlag{
			Name:        "listenUrl",
			Usage:       "grpc listen url",
			Destination: &config.ListenUrl,
		}),
		altsrc.NewStringSliceFlag(&cli.StringSliceFlag{
			Name:        "dirs",
			Usage:       "dirs",
			Destination: &slices,
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
	
	
	 initFileSourceConf := func(flags []cli.Flag) func(*cli.Context) error {
		return func(c *cli.Context) error {
			path := c.String("config")
			if path == "" {
				return nil
			}
			_, err := os.Stat(path)
			if os.IsNotExist(err) {
				panic(fmt.Sprintf("can not open config file %s", path))
			} else if err != nil {
				return err
			}
			return altsrc.InitInputSourceWithContext(flags, altsrc.NewTomlSourceFromFlagFunc("config"))(c)
		}
	}
	
	app := &cli.App{
		Before: initFileSourceConf(flags),
		Flags: flags,
	}


	if err := app.Run(os.Args); err != nil {
		panic(err.Error())
	}

	config.Dirs = slices.Value()
	return &config
}
