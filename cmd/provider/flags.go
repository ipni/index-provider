package main

import (
	"github.com/urfave/cli/v2"
)

var indexerFlag = &cli.StringFlag{
	Name:     "indexer",
	Usage:    "Host or host:port of indexer to use",
	Aliases:  []string{"i"},
	Required: true,
}

var addrFlag = &cli.StringSliceFlag{
	Name:     "addr",
	Usage:    `Provider address as multiaddr string, example: "/ip4/127.0.0.1/tcp/3103"`,
	Aliases:  []string{"a"},
	Required: true,
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
	optionalCarPathFlagValue string
	optionalCarPathFlag      = &cli.StringFlag{
		Name:        "input",
		Aliases:     []string{"i"},
		Usage:       "The CAR file path.",
		Destination: &optionalCarPathFlagValue,
	}
)

var (
	adminAPIFlagValue string
	adminAPIFlag      = &cli.StringFlag{
		Name:        "listen-admin",
		Usage:       "Admin HTTP API listen address",
		Aliases:     []string{"l"},
		EnvVars:     []string{"PROVIDER_LISTEN_ADMIN"},
		Value:       "http://localhost:3102",
		Destination: &adminAPIFlagValue,
	}
)
