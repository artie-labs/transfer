package motherduck

import (
	"context"
	"fmt"

	goSql "database/sql"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"
	jsoniter "github.com/json-iterator/go"

	"github.com/artie-labs/transfer/clients/motherduck/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func BuildDSN(token string) string {
	return fmt.Sprintf("md:?motherduck_token=%s&custom_user_agent=artie-transfer", token)
}

type Store struct {
	dsn       string
	client    *ducktape.Client
	configMap *types.DestinationTableConfigMap
	config    config.Config
}

func LoadStore(cfg config.Config) (*Store, error) {
	return &Store{
		dsn:       BuildDSN(cfg.MotherDuck.Token),
		client:    ducktape.NewClient(cfg.MotherDuck.DucktapeURL),
		configMap: &types.DestinationTableConfigMap{},
		config:    cfg,
	}, nil
}

func (s Store) dialect() dialect.DuckDBDialect {
	return dialect.DuckDBDialect{}
}

func (s Store) Dialect() sql.Dialect {
	return s.dialect()
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) IsOLTP() bool {
	return false
}

func (s Store) Begin() (*goSql.Tx, error) {
	return nil, fmt.Errorf("not implemented: Begin")
}

func (s Store) QueryContextHttp(ctx context.Context, query string, args ...any) (*ducktape.QueryResponse, error) {
	request := ducktape.QueryRequest{
		Query: query,
		Args:  args,
	}
	response, err := s.client.Query(ctx, request, s.dsn, func(r ducktape.QueryRequest) ([]byte, error) {
		return json.Marshal(r)
	}, func(r []byte) (*ducktape.QueryResponse, error) {
		var resp ducktape.QueryResponse
		if err := json.Unmarshal(r, &resp); err != nil {
			return nil, fmt.Errorf("failed to unmarshal query response: %w", err)
		}
		return &resp, nil
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

// QueryContext is a stub to satisfy the destination.Destination interface
// This should never be called since we override GetTableConfig with our custom implementation
func (s Store) QueryContext(ctx context.Context, query string, args ...any) (*goSql.Rows, error) {
	return nil, fmt.Errorf("QueryContext is not implemented for MotherDuck - use QueryContextHttp methods instead")
}

func (s Store) ExecContext(ctx context.Context, query string, args ...any) (goSql.Result, error) {
	request := ducktape.ExecuteRequest{
		Statements: []ducktape.ExecuteStatement{{Query: query, Args: args}},
	}
	response, err := s.client.Execute(ctx, request, s.dsn, func(r ducktape.ExecuteRequest) ([]byte, error) {
		return json.Marshal(r)
	}, func(r []byte) (*ducktape.ExecuteResponse, error) {
		var response ducktape.ExecuteResponse
		if err := json.Unmarshal(r, &response); err != nil {
			return nil, fmt.Errorf("failed to unmarshal execute response: %w", err)
		}
		return &response, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failure on client side to execute query: %w", err)
	}

	if response.Error != nil {
		return nil, fmt.Errorf("execution failed for duckdb: %s", *response.Error)
	}

	return response, nil
}

func (s Store) IsRetryableError(err error) bool {
	return false
}

func (s Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, pair kafkalib.DatabaseAndSchemaPair, primaryKeys []string, includeArtieUpdatedAt bool) error {
	stagingTableID := shared.BuildStagingTableID(s, pair, tableID)
	dedupeQueries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)

	var request ducktape.ExecuteRequest
	for _, query := range dedupeQueries {
		request.Statements = append(request.Statements, ducktape.ExecuteStatement{Query: query})
	}

	response, err := s.client.Execute(ctx, request, s.dsn, func(r ducktape.ExecuteRequest) ([]byte, error) {
		return json.Marshal(r)
	}, func(r []byte) (*ducktape.ExecuteResponse, error) {
		var response ducktape.ExecuteResponse
		if err := json.Unmarshal(r, &response); err != nil {
			return nil, fmt.Errorf("failed to unmarshal execute response: %w", err)
		}
		return &response, nil
	})
	if err != nil {
		return fmt.Errorf("failure on client side to execute query: %w", err)
	}

	if response.Error != nil {
		return fmt.Errorf("execution failed for duckdb: %s", *response.Error)
	}

	return nil
}

func (s Store) SweepTemporaryTables(ctx context.Context, _ *webhooksclient.Client) error {
	for _, dbAndSchema := range kafkalib.GetUniqueStagingDatabaseAndSchemaPairs(s.config.TopicConfigs()) {
		query, args := s.dialect().BuildSweepQuery(dbAndSchema.Database, dbAndSchema.Schema)

		response, err := s.QueryContextHttp(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query temporary tables: %w", err)
		}

		if response.Error != nil {
			return fmt.Errorf("query failed: %s", *response.Error)
		}

		for _, row := range response.Rows {
			tableName, ok := row["table_name"].(string)
			if !ok {
				continue
			}

			tableSchema, ok := row["table_schema"].(string)
			if !ok {
				continue
			}

			tableID := dialect.NewTableIdentifier(dbAndSchema.Database, tableSchema, tableName)
			if _, err := s.ExecContext(ctx, s.Dialect().BuildDropTableQuery(tableID)); err != nil {
				return fmt.Errorf("failed to drop table %s: %w", tableID.FullyQualifiedName(), err)
			}
		}
	}

	return nil
}

func (s Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	return shared.DropTemporaryTable(ctx, &s, tableID, s.configMap)
}

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client) (bool, error) {
	if err := shared.Merge(ctx, s, tableData, types.MergeOpts{}, whClient); err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, _ bool) error {
	return shared.Append(ctx, s, tableData, whClient, types.AdditionalSettings{})
}

func (s *Store) specificIdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) dialect.TableIdentifier {
	return dialect.NewTableIdentifier(databaseAndSchema.Database, databaseAndSchema.Schema, table)
}

func (s Store) IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return s.specificIdentifierFor(databaseAndSchema, table)
}
