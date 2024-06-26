package shared

import (
	"fmt"
	"log/slog"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	mssqlDialect "github.com/artie-labs/transfer/clients/mssql/dialect"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

func DefaultValue(column columns.Column, dialect sql.Dialect, additionalDateFmts []string) (any, error) {
	if column.DefaultValue() == nil {
		return column.DefaultValue(), nil
	}

	switch column.KindDetails.Kind {
	case typing.Struct.Kind, typing.Array.Kind:
		return dialect.EscapeStruct(fmt.Sprint(column.DefaultValue())), nil
	case typing.ETime.Kind:
		if column.KindDetails.ExtendedTimeDetails == nil {
			return nil, fmt.Errorf("column kind details for extended time is nil")
		}

		extTime, err := ext.ParseFromInterface(column.DefaultValue(), additionalDateFmts)
		if err != nil {
			return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %w", column.DefaultValue(), err)
		}

		switch column.KindDetails.ExtendedTimeDetails.Type {
		case ext.TimeKindType:
			return sql.QuoteLiteral(extTime.String(ext.PostgresTimeFormatNoTZ)), nil
		default:
			return sql.QuoteLiteral(extTime.String(column.KindDetails.ExtendedTimeDetails.Format)), nil
		}
	case typing.EDecimal.Kind:
		val, isOk := column.DefaultValue().(*decimal.Decimal)
		if !isOk {
			return nil, fmt.Errorf("colVal is not type *decimal.Decimal")
		}

		return val.String(), nil
	case typing.String.Kind:
		return sql.QuoteLiteral(fmt.Sprint(column.DefaultValue())), nil
	}

	return column.DefaultValue(), nil
}

func BackfillColumn(dwh destination.DataWarehouse, column columns.Column, tableID sql.TableIdentifier) error {
	if !column.ShouldBackfill() {
		// If we don't need to backfill, don't backfill.
		return nil
	}

	if _, ok := dwh.Dialect().(mssqlDialect.MSSQLDialect); ok {
		// TODO: Support MSSQL column backfill
		return nil
	}

	defaultVal, err := DefaultValue(column, dwh.Dialect(), dwh.AdditionalDateFormats())
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

	_, err = dwh.Exec(query)
	if err != nil {
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
}
