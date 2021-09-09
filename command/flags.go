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

var importCarFlags = []cli.Flag{
	adminAPIFlag,
	carPathFlag,
	metadataFlag,
	keyFlag,
}

var (
	metadataFlagValue string
	metadataFlag      = &cli.StringFlag{
		Name:        "metadata",
		Usage:       "Base64 encoded metadata bytes.",
		Aliases:     []string{"m"},
		Required:    false,
		Destination: &metadataFlagValue,
	}
)

var (
	keyFlagValue string
	keyFlag      = &cli.StringFlag{
		Name:        "key",
		Usage:       "Base64 encoded lookup key to associate with imported CAR.",
		Aliases:     []string{"k"},
		Required:    false,
		Destination: &keyFlagValue,
	}
)

var (
	carPathFlagValue string
	carPathFlag      = &cli.StringFlag{
		Name:        "input",
		Aliases:     []string{"i"},
		Usage:       "Path to the CAR file to import",
		Destination: &carPathFlagValue,
		Required:    true,
	}
)

var (
	adminAPIFlagValue string
	adminAPIFlag      = &cli.StringFlag{
		Name:        "listen-admin",
		Usage:       "Admin HTTP API listen address",
		EnvVars:     []string{"PROVIDER_LISTEN_ADMIN"},
		Destination: &adminAPIFlagValue,
		Required:    true,
	}
)

var (
	carZeroLengthAsEOFFlagValue bool
	carZeroLengthAsEOFFlag      = &cli.BoolFlag{
		Name:        "carZeroLengthAsEOF",
		Aliases:     []string{"cz"},
		Usage:       "Specifies whether zero-length blocks in CAR should be consideted as EOF.",
		Value:       false, // Default to disabled, consistent with go-car/v2 defaults.
		Destination: &carZeroLengthAsEOFFlagValue,
	}
)
