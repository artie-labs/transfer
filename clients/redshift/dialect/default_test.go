package dialect

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func TestRedshiftDialect_BuildBackfillQuery(t *testing.T) {
	_dialect := RedshiftDialect{}

	tableID := NewTableIdentifier("{SCHEMA}", "{TABLE}")
	col := columns.NewColumn("{COLUMN}", typing.String)

	assert.Equal(t, `UPDATE {SCHEMA}."{table}" SET "{column}" = {DEFAULT_VALUE} WHERE "{column}" IS NULL;`, _dialect.BuildBackfillQuery(tableID, _dialect.QuoteIdentifier(col.Name()), "{DEFAULT_VALUE}"))
}
