package shared

import (
	"fmt"
	"log/slog"

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
	case typing.ETime.Kind:
		if err := column.KindDetails.EnsureExtendedTimeDetails(); err != nil {
			return nil, err
		}

		_time, err := ext.ParseFromInterface(column.DefaultValue(), column.KindDetails.ExtendedTimeDetails.Type)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", column.DefaultValue(), err)
		}

		switch column.KindDetails.ExtendedTimeDetails.Type {
		case ext.TimeKindType:
			return sql.QuoteLiteral(_time.Format(ext.PostgresTimeFormatNoTZ)), nil
		default:
			return sql.QuoteLiteral(_time.Format(column.KindDetails.ExtendedTimeDetails.Format)), nil
		}
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
