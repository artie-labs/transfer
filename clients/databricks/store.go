package databricks

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/driverctx"

	"github.com/artie-labs/transfer/clients/databricks/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/values"
)

type Store struct {
	db.Store
	volume    string
	cfg       config.Config
	configMap *types.DestinationTableConfigMap
}

func (s Store) GetConfig() config.Config {
	return s.cfg
}

func (s Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	if !tableID.TemporaryTable() {
		return fmt.Errorf("table %q is not a temporary table, so it cannot be dropped", tableID.FullyQualifiedName())
	}

	if _, err := s.ExecContext(ctx, s.dialect().BuildDropTableQuery(tableID)); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// We'll then clear it from our cache
	s.configMap.RemoveTable(tableID)
	return nil
}

func (s Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	if err := shared.Merge(ctx, s, tableData, types.MergeOpts{}); err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}

func (s Store) Append(ctx context.Context, tableData *optimization.TableData, _ bool) error {
	return shared.Append(ctx, s, tableData, types.AdditionalSettings{})
}

func (s Store) IdentifierFor(databaseAndSchema kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(databaseAndSchema.Database, databaseAndSchema.Schema, table)
}

func (s Store) Dialect() sql.Dialect {
	return s.dialect()
}

func (s Store) dialect() dialect.DatabricksDialect {
	return dialect.DatabricksDialect{}
}

func (s Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	stagingTableID := shared.TempTableID(tableID)
	defer func() {
		// Drop the staging table once we're done with the dedupe.
		_ = ddl.DropTemporaryTable(ctx, s, stagingTableID, false)
	}()

	for _, query := range s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt) {
		// Databricks doesn't support transactions, so we can't wrap this in a transaction.
		if _, err := s.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
	}

	return nil
}

func (s Store) GetTableConfig(ctx context.Context, tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	return shared.GetTableCfgArgs{
		Destination:           s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		ColumnNameForName:     "col_name",
		ColumnNameForDataType: "data_type",
		ColumnNameForComment:  "comment",
		DropDeletedColumns:    dropDeletedColumns,
	}.GetTableConfig(ctx)
}

func (s Store) LoadDataIntoTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tableID, _ sql.TableIdentifier, opts types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, opts.ColumnSettings, tableID); err != nil {
			return err
		}
	}

	castedTempTableID, ok := tableID.(dialect.TableIdentifier)
	if !ok {
		return fmt.Errorf("failed to cast temp table ID to TableIdentifier")
	}

	file := NewFileFromTableID(castedTempTableID, s.volume)
	fp, err := s.writeTemporaryTableFile(tableData, file.Name())
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	defer func() {
		// In the case where PUT or COPY fails, we'll at least delete the temporary file.
		if deleteErr := os.RemoveAll(fp); deleteErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", deleteErr), slog.String("filePath", fp))
		}
	}()

	ctx = driverctx.NewContextWithStagingInfo(ctx, []string{"/var", "tmp"})
	putCommand := fmt.Sprintf("PUT '%s' INTO '%s' OVERWRITE", fp, file.DBFSFilePath())
	if _, err = s.ExecContext(ctx, putCommand); err != nil {
		return fmt.Errorf("failed to run PUT INTO for temporary table: %w", err)
	}

	defer func() {
		if _, err = s.ExecContext(ctx, s.dialect().BuildRemoveFileFromVolumeQuery(file.FilePath())); err != nil {
			slog.Warn("Failed to delete file from volume, it will be garbage collected later",
				slog.Any("err", err),
				slog.String("filePath", file.FilePath()),
			)
		}
	}()

	var sourceColumns []string
	var targetColumns []string
	for idx, column := range tableData.ReadOnlyInMemoryCols().ValidColumns() {
		ordinalColumn := fmt.Sprintf("_c%d", idx)
		switch column.KindDetails.Kind {
		case typing.Array.Kind:
			ordinalColumn = fmt.Sprintf(`PARSE_JSON(%s)`, ordinalColumn)
		}

		sourceColumns = append(sourceColumns, ordinalColumn)
		targetColumns = append(targetColumns, column.Name())
	}

	copyCommand := s.dialect().BuildCopyIntoQuery(tableID, targetColumns, sourceColumns, file.DBFSFilePath())
	result, err := s.ExecContext(ctx, copyCommand)
	if err != nil {
		return fmt.Errorf("failed to run COPY INTO for temporary table: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if expectedRows := int64(tableData.NumberOfRows()); rows != expectedRows {
		return fmt.Errorf("rows affected mismatch, expected: %d, got: %d", expectedRows, rows)
	}

	return nil
}

func castColValStaging(colVal any, colKind typing.KindDetails, _ config.SharedDestinationSettings) (shared.ValueConvertResponse, error) {
	if colVal == nil {
		return shared.ValueConvertResponse{Value: constants.NullValuePlaceholder}, nil
	}

	value, err := values.ToString(colVal, colKind)
	if err != nil {
		return shared.ValueConvertResponse{}, err
	}

	return shared.ValueConvertResponse{Value: value}, nil
}

func (s Store) writeTemporaryTableFile(tableData *optimization.TableData, fileName string) (string, error) {
	tempTableDataFile := shared.NewTemporaryDataFileWithFileName(fileName)
	file, _, err := tempTableDataFile.WriteTemporaryTableFile(tableData, castColValStaging, s.cfg.SharedDestinationSettings)
	if err != nil {
		return "", fmt.Errorf("failed to write temporary table file: %w", err)
	}

	return file.FilePath, nil
}

func (s Store) SweepTemporaryTables(ctx context.Context) error {
	tcs := s.cfg.TopicConfigs()
	ctx = driverctx.NewContextWithStagingInfo(ctx, []string{"/var", "tmp"})
	// Remove the temporary files from volumes
	for _, tc := range tcs {
		rows, err := s.QueryContext(ctx, s.dialect().BuildSweepFilesFromVolumesQuery(tc.Database, tc.GetStagingSchema(), s.volume))
		if err != nil {
			return fmt.Errorf("failed to sweep files from volumes: %w", err)
		}

		files, err := sql.RowsToObjects(rows)
		if err != nil {
			return fmt.Errorf("failed to convert rows to objects: %w", err)
		}

		for _, _file := range files {
			file, err := NewFile(_file)
			if err != nil {
				return err
			}

			if file.ShouldDelete() {
				if _, err = s.ExecContext(ctx, s.dialect().BuildRemoveFileFromVolumeQuery(file.FilePath())); err != nil {
					return fmt.Errorf("failed to delete file: %w", err)
				}
			}
		}
	}

	// Delete the temporary tables
	return shared.Sweep(ctx, s, tcs, s.dialect().BuildSweepQuery)
}

func LoadStore(cfg config.Config) (Store, error) {
	store, err := db.Open("databricks", cfg.Databricks.DSN())
	if err != nil {
		return Store{}, err
	}

	return Store{
		Store:     store,
		cfg:       cfg,
		volume:    cfg.Databricks.Volume,
		configMap: &types.DestinationTableConfigMap{},
	}, nil
}
