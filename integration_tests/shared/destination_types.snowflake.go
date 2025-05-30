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

func SnowflakeCreateTable(ctx context.Context, dest destination.Destination, tableID sql.TableIdentifier) error {
	query := dest.Dialect().BuildCreateTableQuery(tableID, false, []string{
		"c_number_38_0 NUMBER(38, 0)",
		"c_numeric_5_2 NUMERIC(5, 2)",
		"c_decimal_5_0 DECIMAL(5, 0)",
		"c_decimal_5_2 DECIMAL(5, 2)",
		"c_float FLOAT",
		"c_float4 FLOAT4",
		"c_float8 FLOAT8",
		"c_double DOUBLE",
		"c_double_precision DOUBLE PRECISION",
		"c_real REAL",
		"c_int INT",
		"c_integer INTEGER",
		"c_bigint BIGINT",
		"c_smallint SMALLINT",
		"c_tinyint TINYINT",
		"c_byteint BYTEINT",
		"c_boolean BOOLEAN",
		"c_variant VARIANT",
		"c_object OBJECT",
		"c_array ARRAY",
		"c_timestamp_ltz TIMESTAMP_LTZ",
		"c_timestamp_tz TIMESTAMP_TZ",
		"c_timestamp TIMESTAMP",
		"c_datetime DATETIME",
		"c_timestamp_ntz TIMESTAMP_NTZ",
		"c_time TIME",
		"c_date DATE",
		"c_varchar VARCHAR",
		"c_varchar_5 VARCHAR(5)",
		"c_char CHAR",
		"c_char_5 CHAR(5)",
		"c_character CHARACTER",
		"c_character_5 CHARACTER(5)",
		"c_string STRING",
		"c_text TEXT",
	})

	_, err := dest.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func SnowflakeAssertColumns(ctx context.Context, dest destination.Destination, tableID sql.TableIdentifier) error {
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

		fmt.Println("row", row)

		columnName, err := maputil.GetTypeFromMap[string](row, "name")
		if err != nil {
			return fmt.Errorf("failed to get column name: %w", err)
		}

		columnType, err := maputil.GetTypeFromMap[string](row, "type")
		if err != nil {
			return fmt.Errorf("failed to get column type: %w", err)
		}

		kd, err := dest.Dialect().KindForDataType(columnType)
		if err != nil {
			return fmt.Errorf("failed to get kind for data type: %w", err)
		}

		foundCols = append(foundCols, columns.NewColumn(columnName, kd))
	}

	if len(foundCols) != 21 {
		return fmt.Errorf("expected 21 columns, got %d", len(foundCols))
	}

	for _, col := range foundCols {
		switch col.Name() {
		case "c_char":
			if err := assertEqual("c_char", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_char", *col.KindDetails.OptionalStringPrecision, int32(1)); err != nil {
				return err
			}
		case "c_char_5":
			if err := assertEqual("c_char_5", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_char_5", *col.KindDetails.OptionalStringPrecision, int32(5)); err != nil {
				return err
			}
		case "c_varchar":
			if err := assertEqual("c_varchar", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_varchar", *col.KindDetails.OptionalStringPrecision, int32(1)); err != nil {
				return err
			}
		case "c_varchar_5":
			if err := assertEqual("c_varchar_5", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_varchar_5", *col.KindDetails.OptionalStringPrecision, int32(5)); err != nil {
				return err
			}
		case "c_nvarchar":
			if err := assertEqual("c_nvarchar", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_nvarchar", *col.KindDetails.OptionalStringPrecision, int32(1)); err != nil {
				return err
			}
		case "c_nvarchar_5":
			if err := assertEqual("c_nvarchar_5", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_nvarchar_5", *col.KindDetails.OptionalStringPrecision, int32(5)); err != nil {
				return err
			}
		case "c_nchar":
			if err := assertEqual("c_nchar", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_nchar", *col.KindDetails.OptionalStringPrecision, int32(1)); err != nil {
				return err
			}
		case "c_nchar_5":
			if err := assertEqual("c_nchar_5", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_nchar_5", *col.KindDetails.OptionalStringPrecision, int32(5)); err != nil {
				return err
			}
		case "c_ntext":
			if err := assertEqual("c_ntext", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}

			// That's the max size: https://learn.microsoft.com/en-us/sql/t-sql/data-types/ntext-text-and-image-transact-sql?view=sql-server-ver17#text
			if err := assertEqual("c_ntext", *col.KindDetails.OptionalStringPrecision, int32(1073741823)); err != nil {
				return err
			}
		case "c_decimal_5_0":
			if err := assertEqual("c_decimal_5_0", col.KindDetails.Kind, typing.EDecimal.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_decimal_5_0", int(col.KindDetails.ExtendedDecimalDetails.Precision()), 5); err != nil {
				return err
			}
			if err := assertEqual("c_decimal_5_0", int(col.KindDetails.ExtendedDecimalDetails.Scale()), 0); err != nil {
				return err
			}
		case "c_decimal_5_2":
			if err := assertEqual("c_decimal_5_2", col.KindDetails.Kind, typing.EDecimal.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_decimal_5_2", int(col.KindDetails.ExtendedDecimalDetails.Precision()), 5); err != nil {
				return err
			}
			if err := assertEqual("c_decimal_5_2", int(col.KindDetails.ExtendedDecimalDetails.Scale()), 2); err != nil {
				return err
			}
		case "c_numeric_5_0":
			if err := assertEqual("c_numeric_5_0", col.KindDetails.Kind, typing.EDecimal.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_numeric_5_0", int(col.KindDetails.ExtendedDecimalDetails.Precision()), 5); err != nil {
				return err
			}
			if err := assertEqual("c_numeric_5_0", int(col.KindDetails.ExtendedDecimalDetails.Scale()), 0); err != nil {
				return err
			}
		case "c_numeric_5_2":
			if err := assertEqual("c_numeric_5_2", col.KindDetails.Kind, typing.EDecimal.Kind); err != nil {
				return err
			}
			if err := assertEqual("c_numeric_5_2", int(col.KindDetails.ExtendedDecimalDetails.Precision()), 5); err != nil {
				return err
			}
			if err := assertEqual("c_numeric_5_2", int(col.KindDetails.ExtendedDecimalDetails.Scale()), 2); err != nil {
				return err
			}
		case "c_float":
			if err := assertEqual("c_float", col.KindDetails.Kind, typing.Float.Kind); err != nil {
				return err
			}
		case "c_real":
			if err := assertEqual("c_real", col.KindDetails.Kind, typing.Float.Kind); err != nil {
				return err
			}
		case "c_datetime":
			if err := assertEqual("c_datetime", col.KindDetails.Kind, typing.TimestampNTZ.Kind); err != nil {
				return err
			}
		case "c_datetime2":
			if err := assertEqual("c_datetime2", col.KindDetails.Kind, typing.TimestampNTZ.Kind); err != nil {
				return err
			}
		case "c_time":
			if err := assertEqual("c_time", col.KindDetails.Kind, typing.Time.Kind); err != nil {
				return err
			}
		case "c_date":
			if err := assertEqual("c_date", col.KindDetails.Kind, typing.Date.Kind); err != nil {
				return err
			}
		case "c_bit":
			if err := assertEqual("c_bit", col.KindDetails.Kind, typing.Boolean.Kind); err != nil {
				return err
			}
		case "c_text":
			if err := assertEqual("c_text", col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}

			// https://learn.microsoft.com/en-us/sql/t-sql/data-types/ntext-text-and-image-transact-sql?view=sql-server-ver17#text
			if err := assertEqual("c_text", *col.KindDetails.OptionalStringPrecision, int32(2147483647)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected column: %q", col.Name())
		}
	}

	return nil
}
