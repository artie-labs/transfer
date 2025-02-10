package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

// DataTypeForKind returns the SparkSQL/Iceberg column data type for a given `typing.KindDetails`.
// Use this for CREATE TABLE or ALTER TABLE ADD COLUMNS statements.
//
// For arrays or structs, we often have to specify the element type. If you do not know the exact
// nested schema, you may store them as strings or generic types, e.g., `ARRAY<STRING>` or `STRING`.
func (IcebergDialect) DataTypeForKind(
	kindDetails typing.KindDetails,
	_ bool, // isPhysicalNullable (ignored here, but you could add "NOT NULL" in some SQL dialects)
	settings config.SharedDestinationColumnSettings,
) string {

	switch kindDetails.Kind {
	case typing.Float.Kind:
		// Spark SQL double precision type
		return "DOUBLE"

	case typing.Array.Kind:
		// By default, store arrays as ARRAY<STRING>. You could also attempt
		// to detect an inner type if you have more metadata.
		return "ARRAY<STRING>"

	case typing.Struct.Kind:
		// In Spark, an actual STRUCT column would need a typed definition, like:
		//   STRUCT<col1: INT, col2: STRING>
		// If you do not know the subfields, you could store it as a JSON string instead:
		//   STRING
		//
		// This example just stores it as a STRING to avoid complex schema evolution.
		// Adjust if you are able to parse the struct sub-schema and generate a full STRUCT<...>.
		return "STRING"

	case typing.Date.Kind:
		return "DATE"

	case typing.Time.Kind:
		// Spark does not have a native TIME type in older versions; newer versions (3.2+) do.
		// If your environment supports it, you can return "TIME". Otherwise, store as STRING.
		// For demonstration, we'll just return "STRING" to avoid issues:
		// return "TIME" // if you know your Spark version supports it
		return "STRING"

	case typing.TimestampNTZ.Kind:
		// Spark 3.4+ has TIMESTAMP_NTZ. But in many Spark versions, a TIMESTAMP
		// is stored without an explicit zone. We'll return "TIMESTAMP" by default.
		return "TIMESTAMP"

	case typing.TimestampTZ.Kind:
		// Spark does not truly store time zone data in a TIMESTAMP type (unlike some databases).
		// Typically it's either "TIMESTAMP" plus a separate offset or just "TIMESTAMP" (UTC).
		// We'll map it to "TIMESTAMP" as well.
		return "TIMESTAMP"

	case typing.EDecimal.Kind:
		// For extended decimal, we need a DECIMAL(precision, scale) in Spark/Iceberg.
		// We can emulate the BigQuery approach and define a method like .SparkDecimalKind(...) if you like,
		// or inline it here. Example fallback to DECIMAL(38, 18) if we have no other info:
		return kindDetails.ExtendedDecimalDetails.IcebergKind()
	default:
		// typing.Integer, Boolean, String, or any other leftover type
		switch kindDetails.Kind {
		case typing.Integer.Kind:
			// Use BIGINT or INT. Spark typically uses INT for smaller columns, BIGINT for 64-bit.
			// "INT" is 32-bit signed, "BIGINT" is 64-bit. Adjust as needed.
			return "BIGINT"

		case typing.Boolean.Kind:
			return "BOOLEAN"

		case typing.String.Kind:
			return "STRING"
		}

		// If nothing matched, fall back to STRING or error
		return "STRING"
	}
}

// KindForDataType parses a SparkSQL (Iceberg) column definition into our internal `typing.KindDetails`.
//
// Example Spark types include:
//   - INT, INTEGER, BIGINT
//   - FLOAT, DOUBLE
//   - DECIMAL(10, 2)
//   - STRING
//   - BOOLEAN
//   - ARRAY<...>, STRUCT<...>, MAP<...>
//   - TIMESTAMP
//   - DATE
//   - (TIME in some newer Spark versions)
//
// We do a simplified parse similar to the BigQuery approach, using `sql.ParseDataTypeDefinition`.
func (IcebergDialect) KindForDataType(rawType string, _ string) (typing.KindDetails, error) {
	// Normalize
	lowerType, parameters, err := sql.ParseDataTypeDefinition(strings.ToLower(rawType))
	if err != nil {
		return typing.Invalid, err
	}

	// For example, if we see "array<struct<...>>", then `lowerType` is "array",
	// and the bracketed part is in `parameters`. Same for "decimal(38,18)".

	// If the raw type is "decimal(precision, scale)", it becomes:
	//   lowerType = "decimal", parameters = []string{ "precision", "scale" }

	// We can truncate <...> if needed:
	baseType := lowerType
	if idx := strings.Index(baseType, "<"); idx > 0 {
		baseType = strings.TrimSpace(baseType[:idx])
	}

	switch baseType {
	// Integers
	case "int", "integer", "bigint":
		return typing.Integer, nil

	// Floats
	case "float", "double":
		return typing.Float, nil

	// Decimals
	case "decimal":
		// decimal(precision, scale)
		if len(parameters) == 0 {
			// e.g., "DECIMAL" with no precision => default or variable
			return typing.EDecimal, nil
		}
		// parse numeric(precision,scale)
		return typing.ParseNumeric(parameters)

	// Booleans
	case "bool", "boolean":
		return typing.Boolean, nil

	// Strings
	case "string", "varchar", "char":
		return typing.String, nil

	// Arrays
	case "array":
		// The child type might be in parameters. If you do deeper introspection,
		// you might recursively parse that as well. For now, just return `typing.Array`.
		return typing.Array, nil

	// Structs
	case "struct":
		return typing.Struct, nil

	// Map (if we want to treat it as struct or array of key-values)
	case "map":
		// Up to you how you handle map. We might treat it as a `Struct` in your system, or a separate type.
		// For simplicity, treat as `Struct` (similar to BQ JSON).
		return typing.Struct, nil

	// Date / Time
	case "date":
		return typing.Date, nil
	case "time":
		// If Spark version < 3.2, "TIME" might not exist or is a partial. We'll still map to `typing.Time`.
		return typing.Time, nil

	// Timestamps
	case "timestamp":
		// Spark doesn't store time zone, but let's treat it as typing.TimestampTZ or NTZ
		// depending on your preference. For consistency with the BQ dialect, we can do:
		return typing.TimestampTZ, nil

	default:
		return typing.Invalid, fmt.Errorf("unsupported or unknown SparkSQL data type: %q", rawType)
	}
}
