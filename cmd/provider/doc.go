/*

Provider CLI can be used to start an index-provider daemon that runs a reference implementation of
the provider interface, with ability to publish advertisements by importing/removing CAR files
and server content retrieval via DataTransfer GraphSync protocol.

Usage:
	NAME:
	   provider - Indexer Reference Provider Implementation

	USAGE:
	   provider [global options] command [command options] [arguments...]

	VERSION:
	   v0.0.0+unknown

	COMMANDS:
	   daemon             Starts a reference provider
	   find               Query an indexer for indexed content
	   index              Push a single content index into an indexer
	   init               Initialize reference provider config file and identity
	   connect            Connects to an indexer through its multiaddr
	   import, i          Imports sources of multihashes to the index provider.
	   register           Register provider information with an indexer that trusts the provider
	   remove, rm         Removes previously advertised multihashes by the provider.
	   verify-ingest, vi  Verifies ingestion of multihashes to an indexer node from a Lotus miner, CAR file or a CARv2 Index
	   list               Lists advertisements
	   help, h            Shows a list of commands or help for one command

	GLOBAL OPTIONS:
	   --help, -h     show help (default: false)
	   --version, -v  print the version (default: false)

To run a provider daemon it must first be initialized. To initialize the provider, execute:
	provider init

Initialization generates a default configuration for the provider instance along with a randomly
generated identity keypair. The configuration is stored at user home
under ".index-provider/config" in JSON format. The root configuration path can be overridden by
setting the "PROVIDER_PATH" environment variable.

Once initialized, the daemon can be started by executing:
	provider daemon

The running daemon allows the advertisement for new content to indexer nodes and retrieval of content
over graphsync. It also exposes an administrative HTTP endpoint at "" that
allows

Additionally, it starts an admin HTTP server at "http://localhost:3102" that enables administrative
operations using the "provider" CLI tool.

To advertise the availability of content by the daemon to indexer nodes, run:
	provider import car -l http://localhost:3102 -i <path-to-car-file>

This command will generate a new advertisement for the list of multihashes in the CAR file and
publishes that advertisement onto the configured gossipsub channel for indexer nodes to see.
Additionally, it makes the CAR content available for retrieval over GraphSync.

Similarly to advertise that the content is no longer available for retrieval by the daemon, use
"provider remove car" command.

For a full list of available commands and options run:
	provider -h

*/
package main
