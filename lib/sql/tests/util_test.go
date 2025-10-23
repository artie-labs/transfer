package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"

	redshiftDialect "github.com/artie-labs/transfer/clients/redshift/dialect"
	"github.com/artie-labs/transfer/lib/sql"
)

func TestQuoteIdentifiers(t *testing.T) {
	assert.Equal(t, []string{}, sql.QuoteIdentifiers([]string{}, redshiftDialect.RedshiftDialect{}))
	assert.Equal(t, []string{`"a"`, `"b"`, `"c"`}, sql.QuoteIdentifiers([]string{"a", "b", "c"}, redshiftDialect.RedshiftDialect{}))
}
