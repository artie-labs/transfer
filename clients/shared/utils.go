package shared

import (
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func BackfillColumn(cfg config.Config, dwh destination.DataWarehouse, column columns.Column, fqTableName string) error {
	if dwh.Label() == constants.BigQuery {
		return fmt.Errorf("bigquery does not use this method")
	}

	if !column.ShouldBackfill() {
		// If we don't need to backfill, don't backfill.
		return nil
	}

	additionalDateFmts := cfg.SharedTransferConfig.TypingSettings.AdditionalDateFormats
	defaultVal, err := column.DefaultValue(&columns.DefaultValueArgs{Escape: true, DestKind: dwh.Label()}, additionalDateFmts)
	if err != nil {
		return fmt.Errorf("failed to escape default value: %w", err)
	}

	uppercaseEscNames := cfg.SharedDestinationConfig.UppercaseEscapedNames
	escapedCol := column.Name(uppercaseEscNames, &sql.NameArgs{Escape: true, DestKind: dwh.Label()})

	// TODO: This is added because `default` is not technically a column that requires escaping, but it is required when it's in the where clause.
	// Once we escape everything by default, we can remove this patch of code.
	additionalEscapedCol := escapedCol
	if additionalEscapedCol == "default" && dwh.Label() == constants.Snowflake {
		// It should be uppercase because Snowflake's default column is uppercase and since it's not a reserved column name, it uses the default setting.
		additionalEscapedCol = `"DEFAULT"`
	}

	query := fmt.Sprintf(`UPDATE %s SET %s = %v WHERE %s IS NULL;`,
		// UPDATE table SET col = default_val WHERE col IS NULL
		fqTableName, escapedCol, defaultVal, additionalEscapedCol,
	)
	slog.Info("Backfilling column",
		slog.String("colName", column.RawName()),
		slog.String("query", query),
		slog.String("table", fqTableName),
	)

	_, err = dwh.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to backfill, err: %w, query: %v", err, query)
	}

	query = fmt.Sprintf(`COMMENT ON COLUMN %s.%s IS '%v';`, fqTableName, escapedCol, `{"backfilled": true}`)
	_, err = dwh.Exec(query)
	return err
}
