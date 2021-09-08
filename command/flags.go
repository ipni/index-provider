package command

import (
	"github.com/urfave/cli/v2"
)

var DaemonFlags = []cli.Flag{
	phFlag,
}

var InitFlags = []cli.Flag{
	phFlag,
}

var connectFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "indexermaddr",
		Usage:    "Indexer multiaddr to connect",
		Aliases:  []string{"imaddr"},
		Required: true,
	},
	adminAPIFlag,
}

var adminAPIFlag = &cli.StringFlag{
	Name:     "listen-admin",
	Usage:    "Admin HTTP API listen address",
	EnvVars:  []string{"PROVIDER_LISTEN_ADMIN"},
	Required: false,
}

var phFlag = &cli.BoolFlag{
	Name:     "placeholder",
	Usage:    "Placeholder Flag to run provider",
	Aliases:  []string{"ph"},
	Value:    false,
	Required: false,
}
