package dialect

import (
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing"
)

func (IcebergDialect) DataTypeForKind(
	kindDetails typing.KindDetails,
	// Primary key doesn't matter for Iceberg
	_ bool,
	settings config.SharedDestinationColumnSettings,
) string {
	// TODO:
	panic("not implemented")
}

func (IcebergDialect) KindForDataType(rawType string, _ string) (typing.KindDetails, error) {
	// TODO:
	panic("not implemented")
}
