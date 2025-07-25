package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	redshiftDialect "github.com/artie-labs/transfer/clients/redshift/dialect"
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
		value, err := json.Marshal(column.DefaultValue())
		if err != nil {
			return nil, fmt.Errorf("failed to marshal default value: %w", err)
		}

		return dialect.EscapeStruct(string(value)), nil
	case typing.Date.Kind:
		_time, err := ext.ParseDateFromAny(column.DefaultValue())
		if err != nil {
			return nil, fmt.Errorf("failed to cast colVal as time.Time, colVal: '%v', err: %w", column.DefaultValue(), err)
		}

		return sql.QuoteLiteral(_time.Format(time.DateOnly)), nil
	case typing.Time.Kind:
		_time, err := ext.ParseTimeFromAny(column.DefaultValue())
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: '%v', err: %w", column.DefaultValue(), err)
		}

		return sql.QuoteLiteral(_time.Format(ext.PostgresTimeFormatNoTZ)), nil
	case typing.TimestampNTZ.Kind:
		_time, err := ext.ParseTimestampNTZFromAny(column.DefaultValue())
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: '%v', err: %w", column.DefaultValue(), err)
		}

		return sql.QuoteLiteral(_time.Format(ext.RFC3339NoTZ)), nil
	case typing.TimestampTZ.Kind:
		_time, err := ext.ParseTimestampTZFromAny(column.DefaultValue())
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: '%v', err: %w", column.DefaultValue(), err)
		}

		return sql.QuoteLiteral(_time.Format(time.RFC3339Nano)), nil
	case typing.EDecimal.Kind:
		if column.KindDetails.ExtendedDecimalDetails == nil || column.KindDetails.ExtendedDecimalDetails.Scale() == 0 {
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
	case
		typing.Boolean.Kind,
		typing.Integer.Kind,
		typing.Float.Kind,
		typing.EDecimal.Kind:
		return fmt.Sprint(column.DefaultValue()), nil
	default:
		return nil, fmt.Errorf("unsupported default value type: %q", column.KindDetails.Kind)
	}

}

func BackfillColumn(ctx context.Context, dest destination.Destination, column columns.Column, tableID sql.TableIdentifier) error {
	if envVar := os.Getenv("DISABLE_DEFAULT_VAL_BACKFILL"); envVar != "" {
		disable, err := strconv.ParseBool(envVar)
		if err != nil {
			return fmt.Errorf("failed to parse DISABLE_DEFAULT_VAL_BACKFILL: %w", err)
		}

		if disable {
			return nil
		}
	}

	dialect := dest.Dialect()
	switch dialect.GetDefaultValueStrategy() {
	case sql.Backfill:
		if !column.ShouldBackfill() {
			// If we don't need to backfill, don't backfill.
			return nil
		}

		defaultVal, err := DefaultValue(column, dialect)
		if err != nil {
			return fmt.Errorf("failed to escape default value: %w", err)
		}

		escapedCol := dialect.QuoteIdentifier(column.Name())
		query := fmt.Sprintf(`UPDATE %s as t SET t.%s = %v WHERE t.%s IS NULL;`,
			// UPDATE table as t SET t.col = default_val WHERE t.col IS NULL
			tableID.FullyQualifiedName(), escapedCol, defaultVal, escapedCol,
		)

		if rd, ok := dialect.(redshiftDialect.RedshiftDialect); ok {
			// Redshift UPDATE does not support table aliasing nor do we need it. Redshift will not throw an ambiguous error if the table and column name are the same.
			query = rd.BuildBackfillQuery(tableID, escapedCol, defaultVal)
		}

		slog.Info("Backfilling column",
			slog.String("colName", column.Name()),
			slog.String("query", query),
			slog.String("table", tableID.FullyQualifiedName()),
		)

		if _, err = dest.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to backfill, err: %w, query: %v", err, query)
		}

		query = fmt.Sprintf(`COMMENT ON COLUMN %s.%s IS '%v';`, tableID.FullyQualifiedName(), escapedCol, `{"backfilled": true}`)
		if _, ok := dialect.(bigQueryDialect.BigQueryDialect); ok {
			query = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET OPTIONS (description=`%s`);",
				// ALTER TABLE table ALTER COLUMN col set OPTIONS (description=...)
				tableID.FullyQualifiedName(), escapedCol, `{"backfilled": true}`,
			)
		}

		_, err = dest.ExecContext(ctx, query)
		return err
	case sql.Native:
		// TODO: Support native strat
		return nil
	case sql.NotImplemented:
		// Skip backfill for databases that don't support it (e.g., PostgreSQL)
		return nil
	default:
		return fmt.Errorf("unknown default value strategy: %q", dest.Dialect().GetDefaultValueStrategy())
	}
}
