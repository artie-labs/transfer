package snowflake

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

type SnowflakeDialect struct{}

func (sd SnowflakeDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ToUpper(identifier))
}

func (SnowflakeDialect) EscapeStruct(value string) string {
	return sql.QuoteLiteral(value)
}

func (SnowflakeDialect) DataTypeForKind(kd typing.KindDetails, _ bool) string {
	return typing.KindToSnowflake(kd)
}

func (SnowflakeDialect) KindForDataType(_type string, _ string) typing.KindDetails {
	return typing.SnowflakeTypeToKind(_type)
}

func (SnowflakeDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Snowflake doesn't have column mutations (IF NOT EXISTS)
	return strings.Contains(err.Error(), "already exists")
}
