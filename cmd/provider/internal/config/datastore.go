package config

const (
	defaultDatastoreType = "levelds"
	defaultDatastoreDir  = "datastore"
)

// Datastore tracks the configuration of the datastore.
type Datastore struct {
	// Type is the type of datastore
	Type string
	// Dir is the directory within the config root where the datastore is kept
	Dir string
}

// NewDatastore instantiates a new Datastore config with default values.
func NewDatastore() Datastore {
	return Datastore{
		Type: defaultDatastoreType,
		Dir:  defaultDatastoreDir,
	}
}

// PopulateDefaults replaces zero-values in the config with default values.
func (c *Datastore) PopulateDefaults() {
	if c.Type == "" {
		c.Type = defaultDatastoreType
	}
	if c.Dir == "" {
		c.Dir = defaultDatastoreDir
	}
}
