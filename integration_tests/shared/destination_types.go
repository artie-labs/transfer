package shared

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func RedshiftCreateTable(ctx context.Context, dest destination.Destination, tableID sql.TableIdentifier) error {
	query := dest.Dialect().BuildCreateTableQuery(tableID, false, []string{
		"c_int2", "INT2",
		"c_int4", "INT4",
		"c_int8", "INT8",
		"c_varchar_max", "VARCHAR(MAX)",
		"c_varchar_12345", "VARCHAR(12345)",
		"c_boolean", "BOOLEAN NULL",
		"c_date", "DATE",
		"c_time", "TIME",
		"c_timestamp_ntz", "TIMESTAMP WITHOUT TIME ZONE",
		"c_timestamp_tz", "TIMESTAMP WITH TIME ZONE",
		"c_decimal_10_2", "DECIMAL(10, 2)",
		"c_super", "SUPER",
	})

	_, err := dest.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func RedshiftAssertColumns(ctx context.Context, dest destination.Destination, tableID sql.TableIdentifier) error {
	query, args, err := dest.Dialect().BuildDescribeTableQuery(tableID)
	if err != nil {
		return fmt.Errorf("failed to build describe table query: %w", err)
	}

	sqlRows, err := dest.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query columns: %w", err)
	}

	rows, err := sql.RowsToObjects(sqlRows)
	if err != nil {
		return fmt.Errorf("failed to convert rows to map slice: %w", err)
	}

	var foundCols []columns.Column
	for _, row := range rows {
		columnName, err := maputil.GetTypeFromMap[string](row, "column_name")
		if err != nil {
			return fmt.Errorf("failed to get column name: %w", err)
		}

		columnType, err := maputil.GetTypeFromMap[string](row, "data_type")
		if err != nil {
			return fmt.Errorf("failed to get column type: %w", err)
		}

		kd, err := dest.Dialect().KindForDataType(columnType, "")
		if err != nil {
			return fmt.Errorf("failed to get kind for data type: %w", err)
		}

		foundCols = append(foundCols, columns.NewColumn(columnName, kd))
	}

	if len(foundCols) != 12 {
		return fmt.Errorf("expected 12 columns, got %d", len(foundCols))
	}

	for _, col := range foundCols {
		switch col.Name() {
		case "c_int2":
			if err := assertEqual(col.KindDetails.Kind, typing.Integer.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.KindDetails.OptionalIntegerKind, typing.ToPtr(typing.SmallIntegerKind)); err != nil {
				return err
			}
		case "c_int4":
			if err := assertEqual(col.KindDetails.Kind, typing.Integer.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.KindDetails.OptionalIntegerKind, typing.ToPtr(typing.IntegerKind)); err != nil {
				return err
			}
		case "c_int8":
			if err := assertEqual(col.KindDetails.Kind, typing.Integer.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.KindDetails.OptionalIntegerKind, typing.ToPtr(typing.BigIntegerKind)); err != nil {
				return err
			}
		case "c_varchar_max":
			if err := assertEqual(col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.KindDetails.OptionalStringPrecision, nil); err != nil {
				return err
			}
		case "c_varchar_12345":
			if err := assertEqual(col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.KindDetails.OptionalStringPrecision, typing.ToPtr(int32(12345))); err != nil {
				return err
			}
		case "c_boolean":
			if err := assertEqual(col.KindDetails.Kind, typing.Boolean.Kind); err != nil {
				return err
			}
		case "c_date":
			if err := assertEqual(col.KindDetails.Kind, typing.Date.Kind); err != nil {
				return err
			}
		case "c_time":
			if err := assertEqual(col.KindDetails.Kind, typing.Time.Kind); err != nil {
				return err
			}
		case "c_timestamp_ntz":
			if err := assertEqual(col.KindDetails.Kind, typing.TimestampNTZ.Kind); err != nil {
				return err
			}
		case "c_timestamp_tz":
			if err := assertEqual(col.KindDetails.Kind, typing.TimestampTZ.Kind); err != nil {
				return err
			}
		case "c_decimal_10_2":
			if err := assertEqual(col.KindDetails.Kind, typing.EDecimal.Kind); err != nil {
				return err
			}

			if err := assertEqual(col.KindDetails.ExtendedDecimalDetails.Precision, 10); err != nil {
				return err
			}

			if err := assertEqual(col.KindDetails.ExtendedDecimalDetails.Scale, 2); err != nil {
				return err
			}
		case "c_super":
			if err := assertEqual(col.KindDetails.Kind, typing.Struct.Kind); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected column: %q", col.Name())
		}
	}
}

func assertEqual(actual, expected any) error {
	if actual != expected {
		return fmt.Errorf("expected %v, got %v", expected, actual)
	}

	return nil
}
