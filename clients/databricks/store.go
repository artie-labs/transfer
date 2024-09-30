package databricks

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/values"

	"github.com/artie-labs/transfer/clients/databricks/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	_ "github.com/databricks/databricks-sql-go"
	driverctx "github.com/databricks/databricks-sql-go/driverctx"
)

type Store struct {
	db.Store
	cfg       config.Config
	configMap *types.DwhToTablesConfigMap
}

func describeTableQuery(tableID TableIdentifier) (string, []any) {
	_dialect := dialect.DatabricksDialect{}
	return fmt.Sprintf("DESCRIBE TABLE %s.%s.%s",
		_dialect.QuoteIdentifier(tableID.Database()),
		_dialect.QuoteIdentifier(tableID.Schema()),
		_dialect.QuoteIdentifier(tableID.Table()),
	), nil
}

func (s Store) Merge(tableData *optimization.TableData) error {
	return shared.Merge(s, tableData, types.MergeOpts{})
}

func (s Store) Append(tableData *optimization.TableData, useTempTable bool) error {
	return shared.Append(s, tableData, types.AdditionalSettings{UseTempTable: useTempTable})
}

func (s Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table)
}

func (s Store) Dialect() sql.Dialect {
	return dialect.DatabricksDialect{}
}

func (s Store) Dedupe(tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	panic("not implemented")
}

func (s Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	tableID := NewTableIdentifier(tableData.TopicConfig().Database, tableData.TopicConfig().Schema, tableData.Name())
	query, args := describeTableQuery(tableID)
	return shared.GetTableCfgArgs{
		Dwh:                   s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		Query:                 query,
		Args:                  args,
		ColumnNameForName:     "col_name",
		ColumnNameForDataType: "data_type",
		ColumnNameForComment:  "comment",
		DropDeletedColumns:    tableData.TopicConfig().DropDeletedColumns,
	}.GetTableConfig()
}

func (s Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableID sql.TableIdentifier, _ sql.TableIdentifier, _ types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		tempAlterTableArgs := ddl.AlterTableArgs{
			Dialect:        s.Dialect(),
			Tc:             tableConfig,
			TableID:        tempTableID,
			CreateTable:    true,
			TemporaryTable: true,
			ColumnOp:       constants.Add,
			Mode:           tableData.Mode(),
		}

		if err := tempAlterTableArgs.AlterTable(s, tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
	}

	// Write data into a temporary file
	fp, err := s.writeTemporaryTableFile(tableData, tempTableID)
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	defer func() {
		// In the case where PUT or COPY fails, we'll at least delete the temporary file.
		if deleteErr := os.RemoveAll(fp); deleteErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", deleteErr), slog.String("filePath", fp))
		}
	}()

	castedTempTableID, isOk := tempTableID.(TableIdentifier)
	if !isOk {
		return fmt.Errorf("failed to cast tempTableID to TableIdentifier")
	}

	dbfsFilePath := fmt.Sprintf("dbfs:/Volumes/%s/%s/vol_test/%s.csv", castedTempTableID.Database(), castedTempTableID.Schema(), tempTableID.Table())

	ctx := driverctx.NewContextWithStagingInfo(context.Background(), []string{"/var"})

	// Use the PUT INTO command to upload the file to Databricks
	putCommand := fmt.Sprintf("PUT '%s' INTO '%s' OVERWRITE", fp, dbfsFilePath)
	if _, err = s.ExecContext(ctx, putCommand); err != nil {
		return fmt.Errorf("failed to run PUT INTO for temporary table: %w", err)
	}

	// Use the COPY INTO command to load the data into the temporary table
	copyCommand := fmt.Sprintf("COPY INTO %s BY POSITION FROM '%s' FILEFORMAT = CSV FORMAT_OPTIONS ('delimiter' = '\t', 'header' = 'false')", tempTableID.FullyQualifiedName(), dbfsFilePath)
	if _, err = s.ExecContext(ctx, copyCommand); err != nil {
		return fmt.Errorf("failed to run COPY INTO for temporary table: %w", err)
	}

	return nil
}

func castColValStaging(colVal any, colKind typing.KindDetails) (string, error) {
	if colVal == nil {
		// \\N needs to match NULL_IF(...) from ddl.go
		return `\\N`, nil
	}

	value, err := values.ToString(colVal, colKind)
	if err != nil {
		return "", err
	}

	return value, nil
}

func (s Store) writeTemporaryTableFile(tableData *optimization.TableData, newTableID sql.TableIdentifier) (string, error) {
	fp := filepath.Join(os.TempDir(), fmt.Sprintf("%s.csv", newTableID.FullyQualifiedName()))
	file, err := os.Create(fp)
	if err != nil {
		return "", err
	}

	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = '\t'

	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	for _, value := range tableData.Rows() {
		var row []string
		for _, col := range columns {
			castedValue, castErr := castColValStaging(value[col.Name()], col.KindDetails)
			if castErr != nil {
				return "", castErr
			}

			row = append(row, castedValue)
		}

		if err = writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	writer.Flush()
	return fp, writer.Error()
}

func LoadStore(cfg config.Config) (Store, error) {
	store, err := db.Open("databricks", cfg.Databricks.DSN())
	if err != nil {
		return Store{}, err
	}
	return Store{
		Store:     store,
		cfg:       cfg,
		configMap: &types.DwhToTablesConfigMap{},
	}, nil
}
