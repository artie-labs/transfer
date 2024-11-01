package shared

import (
	"fmt"
	"log/slog"
	"time"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func DefaultValue(column columns.Column, dialect sql.Dialect) (any, error) {
	if column.DefaultValue() == nil {
		return column.DefaultValue(), nil
	}

	switch column.KindDetails.Kind {
	case typing.Struct.Kind, typing.Array.Kind:
		return dialect.EscapeStruct(fmt.Sprint(column.DefaultValue())), nil
	case typing.Date.Kind:
		_time, err := ext.ParseDateFromInterface(column.DefaultValue())
		if err != nil {
			return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: '%v', err: %w", column.DefaultValue(), err)
		}

		return sql.QuoteLiteral(_time.Format(ext.PostgresDateFormat)), nil
	case typing.Time.Kind:
		_time, err := ext.ParseTimeFromInterface(column.DefaultValue())
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: '%v', err: %w", column.DefaultValue(), err)
		}

		return sql.QuoteLiteral(_time.Format(ext.PostgresTimeFormatNoTZ)), nil
	case typing.TimestampNTZ.Kind:
		_time, err := ext.ParseTimestampNTZFromInterface(column.DefaultValue())
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: '%v', err: %w", column.DefaultValue(), err)
		}

		return sql.QuoteLiteral(_time.Format(ext.RFC3339NoTZ)), nil
	case typing.TimestampTZ.Kind:
		_time, err := ext.ParseTimestampTZFromInterface(column.DefaultValue())
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: '%v', err: %w", column.DefaultValue(), err)
		}

		return sql.QuoteLiteral(_time.Format(time.RFC3339Nano)), nil
	case typing.EDecimal.Kind:
		if column.KindDetails.ExtendedDecimalDetails.Scale() == 0 {
			switch column.DefaultValue().(type) {
			case int, int8, int16, int32, int64:
				return fmt.Sprint(column.DefaultValue()), nil
			}
		}

		decimalValue, err := typing.AssertType[*decimal.Decimal](column.DefaultValue())
		if err != nil {
			return nil, err
		}

		return decimalValue.String(), nil
	case typing.String.Kind:
		return sql.QuoteLiteral(fmt.Sprint(column.DefaultValue())), nil
	}

	return column.DefaultValue(), nil
}

func BackfillColumn(dwh destination.DataWarehouse, column columns.Column, tableID sql.TableIdentifier) error {
	switch dwh.Dialect().GetDefaultValueStrategy() {
	case sql.Backfill:
		if !column.ShouldBackfill() {
			// If we don't need to backfill, don't backfill.
			return nil
		}

		defaultVal, err := DefaultValue(column, dwh.Dialect())
		if err != nil {
			return fmt.Errorf("failed to escape default value: %w", err)
		}

		escapedCol := dwh.Dialect().QuoteIdentifier(column.Name())
		query := fmt.Sprintf(`UPDATE %s SET %s = %v WHERE %s IS NULL;`,
			// UPDATE table SET col = default_val WHERE col IS NULL
			tableID.FullyQualifiedName(), escapedCol, defaultVal, escapedCol,
		)
		slog.Info("Backfilling column",
			slog.String("colName", column.Name()),
			slog.String("query", query),
			slog.String("table", tableID.FullyQualifiedName()),
		)

		if _, err = dwh.Exec(query); err != nil {
			return fmt.Errorf("failed to backfill, err: %w, query: %v", err, query)
		}

		query = fmt.Sprintf(`COMMENT ON COLUMN %s.%s IS '%v';`, tableID.FullyQualifiedName(), escapedCol, `{"backfilled": true}`)
		if _, ok := dwh.Dialect().(bigQueryDialect.BigQueryDialect); ok {
			query = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET OPTIONS (description=`%s`);",
				// ALTER TABLE table ALTER COLUMN col set OPTIONS (description=...)
				tableID.FullyQualifiedName(), escapedCol, `{"backfilled": true}`,
			)
		}

		_, err = dwh.Exec(query)
		return err
	case sql.Native:
		// TODO: Support native strat
		return nil
	default:
		return fmt.Errorf("unknown default value strategy: %q", dwh.Dialect().GetDefaultValueStrategy())
	}
}
