package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/gammazero/fsutil"
)

// Config is used to load config files.
type Config struct {
	Identity         Identity
	Datastore        Datastore
	Ingest           Ingest
	ProviderServer   ProviderServer
	AdminServer      AdminServer
	Bootstrap        Bootstrap
	DirectAnnounce   DirectAnnounce
	DelegatedRouting DelegatedRouting
}

const (
	// DefaultPathName is the default config dir name.
	DefaultPathName = ".index-provider"
	// DefaultPathRoot is the path to the default config dir location.
	DefaultPathRoot = "~/" + DefaultPathName
	// DefaultConfigFile is the filename of the configuration file.
	DefaultConfigFile = "config"
	// EnvDir is the environment variable used to change the path root.
	EnvDir = "PROVIDER_PATH"
)

var (
	ErrInitialized    = errors.New("configuration file already exists")
	ErrNotInitialized = errors.New("not initialized")
)

// Filename returns the configuration file path given a configuration root
// directory. If the configuration root directory is empty, use the default.
func Filename(configRoot string) (string, error) {
	return Path(configRoot, DefaultConfigFile)
}

// Marshal configuration with JSON.
func Marshal(value interface{}) ([]byte, error) {
	// need to prettyprint, hence MarshalIndent, instead of Encoder.
	return json.MarshalIndent(value, "", "  ")
}

// Path returns the config file path relative to the configuration root. If an
// empty string is provided for `configRoot`, the default root is used. If
// configFile is an absolute path, then configRoot is ignored.
func Path(configRoot, configFile string) (string, error) {
	if filepath.IsAbs(configFile) {
		return filepath.Clean(configFile), nil
	}
	if configRoot == "" {
		var err error
		configRoot, err = PathRoot()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(configRoot, configFile), nil
}

// PathRoot returns the default configuration root directory.
func PathRoot() (string, error) {
	dir := os.Getenv(EnvDir)
	if dir != "" {
		return dir, nil
	}
	return fsutil.ExpandHome(DefaultPathRoot)
}

// Load reads the json-serialized config at the specified path.
func Load(filePath string) (*Config, error) {
	var err error
	if filePath == "" {
		filePath, err = Filename("")
		if err != nil {
			return nil, err
		}
	}

	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotInitialized
		}
		return nil, err
	}
	defer f.Close()

	// Populate with initial values in case they are not present in config.
	cfg := Config{
		Bootstrap:        NewBootstrap(),
		Datastore:        NewDatastore(),
		Ingest:           NewIngest(),
		AdminServer:      NewAdminServer(),
		ProviderServer:   NewProviderServer(),
		DirectAnnounce:   NewDirectAnnounce(),
		DelegatedRouting: NewDelegatedRouting(),
	}

	if err = json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, err
	}

	// Replace any zero-values with defaults.
	cfg.PopulateDefaults()

	return &cfg, nil
}

// Save writes the json-serialized config to the specified path.
func (c *Config) Save(filePath string) error {
	var err error
	if filePath == "" {
		filePath, err = Filename("")
		if err != nil {
			return err
		}
	}

	err = os.MkdirAll(filepath.Dir(filePath), 0755)
	if err != nil {
		return err
	}

	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	buf, err := Marshal(c)
	if err != nil {
		return err
	}
	_, err = f.Write(buf)
	return err
}

// String returns a pretty-printed json config.
func (c *Config) String() string {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (c *Config) PopulateDefaults() {
	c.AdminServer.PopulateDefaults()
	c.Datastore.PopulateDefaults()
	c.Ingest.PopulateDefaults()
	c.ProviderServer.PopulateDefaults()
	c.DelegatedRouting.PopulateDefaults()
}
