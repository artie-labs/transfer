package shared

import (
	"context"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func SnowflakeCreateTable(ctx context.Context, dest destination.Destination, tableID sql.TableIdentifier) error {
	query := dest.Dialect().BuildCreateTableQuery(tableID, false, config.Replication, []string{
		"c_array ARRAY",
		"c_bigint BIGINT",
		"c_boolean BOOLEAN",
		"c_byteint BYTEINT",
		"c_char CHAR",
		"c_char_5 CHAR(5)",
		"c_character CHARACTER",
		"c_character_5 CHARACTER(5)",
		"c_date DATE",
		"c_datetime DATETIME",
		"c_decimal_5_0 DECIMAL(5, 0)",
		"c_decimal_5_2 DECIMAL(5, 2)",
		"c_double DOUBLE",
		"c_double_precision DOUBLE PRECISION",
		"c_float FLOAT",
		"c_float4 FLOAT4",
		"c_float8 FLOAT8",
		"c_int INT",
		"c_integer INTEGER",
		"c_number_38_0 NUMBER(38, 0)",
		"c_numeric_5_2 NUMERIC(5, 2)",
		"c_object OBJECT",
		"c_real REAL",
		"c_smallint SMALLINT",
		"c_string STRING",
		"c_text TEXT",
		"c_time TIME",
		"c_timestamp TIMESTAMP",
		"c_timestamp_ltz TIMESTAMP_LTZ",
		"c_timestamp_ntz TIMESTAMP_NTZ",
		"c_timestamp_tz TIMESTAMP_TZ",
		"c_tinyint TINYINT",
		"c_variant VARIANT",
		"c_varchar VARCHAR",
		"c_varchar_5 VARCHAR(5)",
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

	if len(foundCols) != 35 {
		return fmt.Errorf("expected 35 columns, got %d", len(foundCols))
	}

	for _, col := range foundCols {
		switch strings.ToLower(col.Name()) {
		case "c_array":
			if err := assertEqual("c_array", col.KindDetails.Kind, typing.Array.Kind); err != nil {
				return err
			}
		case "c_bigint", "c_byteint", "c_int", "c_integer", "c_smallint", "c_tinyint", "c_number_38_0":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.EDecimal.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.Name(), int(col.KindDetails.ExtendedDecimalDetails.Precision()), 38); err != nil {
				return err
			}
			if err := assertEqual(col.Name(), int(col.KindDetails.ExtendedDecimalDetails.Scale()), 0); err != nil {
				return err
			}
		case "c_boolean":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.Boolean.Kind); err != nil {
				return err
			}
		case "c_char", "c_character":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.Name(), int(*col.KindDetails.OptionalStringPrecision), 1); err != nil {
				return err
			}
		case "c_char_5", "c_character_5":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.Name(), int(*col.KindDetails.OptionalStringPrecision), 5); err != nil {
				return err
			}
		case "c_date":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.Date.Kind); err != nil {
				return err
			}
		case "c_datetime", "c_timestamp", "c_timestamp_ntz":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.TimestampNTZ.Kind); err != nil {
				return err
			}
		case "c_timestamp_ltz":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.TimestampTZ.Kind); err != nil {
				return err
			}
		case "c_timestamp_tz":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.TimestampTZ.Kind); err != nil {
				return err
			}
		case "c_decimal_5_0":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.EDecimal.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.Name(), int(col.KindDetails.ExtendedDecimalDetails.Precision()), 5); err != nil {
				return err
			}
			if err := assertEqual(col.Name(), int(col.KindDetails.ExtendedDecimalDetails.Scale()), 0); err != nil {
				return err
			}
		case "c_decimal_5_2", "c_numeric_5_2":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.EDecimal.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.Name(), int(col.KindDetails.ExtendedDecimalDetails.Precision()), 5); err != nil {
				return err
			}
			if err := assertEqual(col.Name(), int(col.KindDetails.ExtendedDecimalDetails.Scale()), 2); err != nil {
				return err
			}
		case "c_float", "c_float4", "c_float8", "c_double", "c_double_precision", "c_real":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.Float.Kind); err != nil {
				return err
			}
		case "c_object":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.Struct.Kind); err != nil {
				return err
			}
		case "c_string", "c_text", "c_varchar":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.Name(), int(*col.KindDetails.OptionalStringPrecision), 16777216); err != nil {
				return err
			}
		case "c_varchar_5":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.String.Kind); err != nil {
				return err
			}
			if err := assertEqual(col.Name(), int(*col.KindDetails.OptionalStringPrecision), 5); err != nil {
				return err
			}
		case "c_time":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.Time.Kind); err != nil {
				return err
			}
		case "c_variant":
			if err := assertEqual(col.Name(), col.KindDetails.Kind, typing.Struct.Kind); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected column: %q", col.Name())
		}
	}

	return nil
}
