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

func MSSQLCreateTable(ctx context.Context, dest destination.Destination, tableID sql.TableIdentifier) error {
	query := dest.Dialect().BuildCreateTableQuery(tableID, false, []string{
		"c_int2 INT2",
		"c_int4 INT4",
		"c_int8 INT8",
		"c_varchar_max VARCHAR(MAX)",
		"c_varchar_12345 VARCHAR(12345)",
		"c_boolean BOOLEAN NULL",
		"c_date DATE",
		"c_time TIME",
		"c_timestamp_ntz TIMESTAMP WITHOUT TIME ZONE",
		"c_timestamp_tz TIMESTAMP WITH TIME ZONE",
		"c_decimal_10_2 DECIMAL(10, 2)",
		"c_super SUPER",
	})

	_, err := dest.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func MSSQLAssertColumns(ctx context.Context, dest destination.Destination, tableID sql.TableIdentifier) error {
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
			if err := assertEqual("c_int2", col.KindDetails.Kind, typing.Integer.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_int2", *col.KindDetails.OptionalIntegerKind, typing.SmallIntegerKind); err != nil {
				return err
			}
		case "c_int4":
			if err := assertEqual("c_int4", col.KindDetails.Kind, typing.Integer.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_int4", *col.KindDetails.OptionalIntegerKind, typing.IntegerKind); err != nil {
				return err
			}
		case "c_int8":
			if err := assertEqual("c_int8", col.KindDetails.Kind, typing.Integer.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_int8", *col.KindDetails.OptionalIntegerKind, typing.BigIntegerKind); err != nil {
				return err
			}
		case "c_varchar_max":
			if err := assertEqual("c_varchar_max", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_varchar_max", *col.KindDetails.OptionalStringPrecision, int32(65535)); err != nil {
				return err
			}
		case "c_varchar_12345":
			if err := assertEqual("c_varchar_12345", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_varchar_12345", *col.KindDetails.OptionalStringPrecision, int32(12345)); err != nil {
				return err
			}
		case "c_boolean":
			if err := assertEqual("c_boolean", col.KindDetails.Kind, typing.Boolean.Kind); err != nil {
				return err
			}
		case "c_date":
			if err := assertEqual("c_date", col.KindDetails.Kind, typing.Date.Kind); err != nil {
				return err
			}
		case "c_time":
			if err := assertEqual("c_time", col.KindDetails.Kind, typing.Time.Kind); err != nil {
				return err
			}
		case "c_timestamp_ntz":
			if err := assertEqual("c_timestamp_ntz", col.KindDetails.Kind, typing.TimestampNTZ.Kind); err != nil {
				return err
			}
		case "c_timestamp_tz":
			if err := assertEqual("c_timestamp_tz", col.KindDetails.Kind, typing.TimestampTZ.Kind); err != nil {
				return err
			}
		case "c_decimal_10_2":
			if err := assertEqual("c_decimal_10_2", col.KindDetails.Kind, typing.EDecimal.Kind); err != nil {
				return err
			}

			if err := assertEqual("c_decimal_10_2", int(col.KindDetails.ExtendedDecimalDetails.Precision()), 10); err != nil {
				return err
			}

			if err := assertEqual("c_decimal_10_2", int(col.KindDetails.ExtendedDecimalDetails.Scale()), 2); err != nil {
				return err
			}
		case "c_super":
			if err := assertEqual("c_super", col.KindDetails.Kind, typing.Struct.Kind); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected column: %q", col.Name())
		}
	}

	return nil
}
