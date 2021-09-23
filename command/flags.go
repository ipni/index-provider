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

var indexerFlag = &cli.StringFlag{
	Name:     "indexer",
	Usage:    "Host or host:port of indexer to use",
	Aliases:  []string{"i"},
	Required: true,
}

var addrFlag = &cli.StringSliceFlag{
	Name:     "addr",
	Usage:    "Provider address as multiaddr string, example: \"/ip4/127.0.0.1/tcp/3103\"",
	Aliases:  []string{"a"},
	Required: true,
}

var findFlags = []cli.Flag{
	indexerFlag,
	&cli.StringSliceFlag{
		Name:     "mh",
		Usage:    "Specify multihash to use as indexer key, multiple OK",
		Required: false,
	},
	&cli.StringSliceFlag{
		Name:     "cid",
		Usage:    "Specify CID to use as indexer key, multiple OK",
		Required: false,
	},
}

var indexFlags = []cli.Flag{
	indexerFlag,
	addrFlag,
	&cli.StringFlag{
		Name:     "mh",
		Usage:    "Specify multihash to use as indexer key",
		Required: false,
	},
	&cli.StringFlag{
		Name:     "cid",
		Usage:    "Specify CID to use as indexer key",
		Required: false,
	},
	&cli.StringFlag{
		Name:     "meta",
		Usage:    "Metadata bytes.",
		Aliases:  []string{"m"},
		Required: false,
	},
	&cli.IntFlag{
		Name:     "proto",
		Usage:    "Specify retrieval protocol ID",
		Required: false,
	},
}

var registerFlags = []cli.Flag{
	indexerFlag,
	addrFlag,
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
