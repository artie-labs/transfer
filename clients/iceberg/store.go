package iceberg

import (
	"fmt"

	"github.com/artie-labs/transfer/clients/iceberg/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/sql"
)

type Store struct {
	catalog string
}

func LoadStore(cfg config.Config) (Store, error) {
	return Store{}, nil
}

func (s Store) Append(data interface{}) error {
	return fmt.Errorf("not implemented")
}

func (s Store) Merge() error {
	return fmt.Errorf("not implemented")
}

func (s Store) IsRetryableError(err error) bool {
	return false
}

func (s Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(s.catalog, topicConfig.Schema, table)
}
