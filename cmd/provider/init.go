package main

import (
	"errors"
	"io/fs"
	"os"

	"github.com/ipni/index-provider/cmd/provider/internal/config"
	"github.com/urfave/cli/v2"
)

var InitCmd = &cli.Command{
	Name:   "init",
	Usage:  "Initialize reference provider config file and identity",
	Action: initCommand,
}

var initFlags = []cli.Flag{}

func initCommand(cctx *cli.Context) error {
	log.Info("Initializing provider config file")

	// Check that the config root exists and it writable.
	configRoot, err := config.PathRoot()
	if err != nil {
		return err
	}

	if err = dirWritable(configRoot); err != nil {
		return err
	}

	configFile, err := config.Filename(configRoot)
	if err != nil {
		return err
	}

	_, err = os.Stat(configFile)
	if !errors.Is(err, fs.ErrNotExist) {
		return config.ErrInitialized
	}

	cfg, err := config.Init(cctx.App.Writer)
	if err != nil {
		return err
	}

	return cfg.Save(configFile)
}
