package s3tables

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
)

type Store struct {
	apacheLivyClient Client
	config           config.Config
}

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, useTempTable bool) error {
	return fmt.Errorf("not implemented")
}

func (s Store) IsRetryableError(_ error) bool {
	return false
}

func (s Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return nil
}

func LoadStore(cfg config.Config) (Store, error) {
	apacheLivyClient, err := NewClient(context.Background(), cfg.S3Tables.ApacheLivyURL)
	if err != nil {
		return Store{}, err
	}

	return Store{config: cfg, apacheLivyClient: apacheLivyClient}, nil
}
