package config

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
)

// Config is used to load config files.
type Config struct {
	Identity       Identity
	Datastore      Datastore
	Ingest         Ingest
	ProviderServer ProviderServer
	AdminServer    AdminServer
	Bootstrap      Bootstrap
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

var log = logging.Logger("config")

func fetchPeerIdFromS3(config *Config, bucket, key string) (error) {
	log.Infof("downloading PeerID configuration from s3://%s/%s", bucket, key)

	ctx := context.TODO()
	cfg, err := awsConfig.LoadDefaultConfig(ctx)

	if err != nil {
		return err
	}

	client := s3.NewFromConfig(cfg)
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key: aws.String(key),
	})

	if err != nil {
		return err
	}

	var peer Identity
	if err = json.NewDecoder(output.Body).Decode(&peer); err != nil {
		return err
	}

	config.Identity = peer

	return nil
}

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
	return homedir.Expand(DefaultPathRoot)
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

	var cfg Config
	if err = json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, err
	}

	peerIdS3Bucket := os.Getenv("PEER_ID_S3_BUCKET")
	peerIdS3File := os.Getenv("PEER_ID_FILE")
	if peerIdS3Bucket != "" && peerIdS3File != "" {
		s3Err := fetchPeerIdFromS3(&cfg, peerIdS3Bucket, peerIdS3File)

		if s3Err != nil {
			return nil, s3Err
		}
	}

	// Replace any zero-values with defaults.
	cfg.Ingest.defaults()

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
