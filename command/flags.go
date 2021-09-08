package command

import (
	"github.com/urfave/cli/v2"
)

var daemonFlags = []cli.Flag{
	carZeroLengthAsEOFFlag,
}

var initFlags = []cli.Flag{}

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

var (
	carZeroLengthAsEOF     bool
	carZeroLengthAsEOFFlag = &cli.BoolFlag{
		Name:        "carZeroLengthAsEOF",
		Aliases:     []string{"cz"},
		Usage:       "Specifies whether zero-length blocks in CAR should be consideted as EOF.",
		Value:       false, // Default to disabled, consistent with go-car/v2 defaults.
		Destination: &carZeroLengthAsEOF,
	}
)
