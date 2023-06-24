package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/dwh"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func BackfillColumn(ctx context.Context, dwh dwh.DataWarehouse, column columns.Column, fqTableName string) error {
	if dwh.Label() == constants.BigQuery {
		return fmt.Errorf("bigquery does not use this method")
	}

	if !column.ShouldBackfill() {
		// If we don't need to backfill, don't backfill.
		return nil
	}

	fqTableName = strings.ToLower(fqTableName)
	defaultArgs := &columns.DefaultValueArgs{
		Escape: true,
	}

	if dwh.Label() == constants.Redshift {
		defaultArgs.Redshift = true
	}

	defaultVal, err := column.DefaultValue(defaultArgs)
	if err != nil {
		return fmt.Errorf("failed to escape default value, err: %v", err)
	}

	escapedCol := column.Name(&columns.NameArgs{Escape: true, DestKind: dwh.Label()})
	query := fmt.Sprintf(`UPDATE %s SET %s = %v WHERE %s IS NULL;`,
		// UPDATE table SET col = default_val WHERE col IS NULL
		fqTableName, escapedCol, defaultVal, escapedCol,
	)
	logger.FromContext(ctx).WithFields(map[string]interface{}{
		"colName": column.Name(nil),
		"query":   query,
		"table":   fqTableName,
	}).Info("backfilling column")

	_, err = dwh.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to backfill, err: %v, query: %v", err, query)
	}

	query = fmt.Sprintf(`COMMENT ON COLUMN %s.%s IS '%v';`, fqTableName, escapedCol, `{"backfilled": true}`)
	_, err = dwh.Exec(query)
	return err
}
