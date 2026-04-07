package config

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/stringutil"
)

type IndexSettings struct {
	NumberOfShards   int `yaml:"numberOfShards"`
	NumberOfReplicas int `yaml:"numberOfReplicas"`
}

type Elasticsearch struct {
	Host          string        `yaml:"host"`
	Username      string        `yaml:"username"`
	Password      string        `yaml:"password"`
	APIKey        string        `yaml:"apiKey"`
	IndexSettings IndexSettings `yaml:"indexSettings"`
}

func (e *Elasticsearch) Validate() error {
	if e == nil {
		return fmt.Errorf("elasticsearch config is nil")
	}

	if stringutil.Empty(e.Host) {
		return fmt.Errorf("elasticsearch host is empty")
	}

	if stringutil.Empty(e.Username, e.Password) && stringutil.Empty(e.APIKey) {
		return fmt.Errorf("either username/password or api key must be provided")
	}

	return nil
}
