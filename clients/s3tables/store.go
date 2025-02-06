package s3tables

import (
	"context"
	"fmt"
	"log"
	"log/slog"

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
	if err := s.apacheLivyClient.ExecContext(ctx, "CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.my_namespace"); err != nil {
		log.Panic("failed to create namespace", slog.Any("err", err))
		return false, fmt.Errorf("failed to create namespace: %w", err)
	}

	if err := s.apacheLivyClient.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS s3tablesbucket.my_namespace.`my_table` ( id INT, name STRING, value INT ) USING iceberg"); err != nil {
		log.Panic("failed to create table", slog.Any("err", err))
		return false, fmt.Errorf("failed to create table: %w", err)
	}

	fmt.Println("create namespace + table success")

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
	apacheLivyClient, err := NewClient(context.Background(), cfg)
	if err != nil {
		return Store{}, err
	}

	return Store{config: cfg, apacheLivyClient: apacheLivyClient}, nil
}
