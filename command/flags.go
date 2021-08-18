package command

import (
	_ "github.com/lib/pq"
	"github.com/urfave/cli/v2"
)

var DaemonFlags = []cli.Flag{
	phFlag,
}
var InitFlags = []cli.Flag{
	phFlag,
}

var phFlag = &cli.BoolFlag{
	Name:     "placeholder",
	Usage:    "Placeholder Flag to run provider",
	Aliases:  []string{"ph"},
	Value:    false,
	Required: false,
}
