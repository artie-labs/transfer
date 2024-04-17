package redshift

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/stretchr/testify/assert"
)

func TestFullyQualifiedName(t *testing.T) {
	tableID := optimization.NewTableIdentifier("database", "schema", "table")

	{
		// With UppercaseEscapedNames: true
		store := Store{
			config: config.Config{
				SharedDestinationConfig: config.SharedDestinationConfig{
					UppercaseEscapedNames: true,
				},
			},
		}
		assert.Equal(t, `schema."TABLE"`, store.ToFullyQualifiedName(tableID, true), "escaped")
		assert.Equal(t, "schema.table", store.ToFullyQualifiedName(tableID, false), "unescaped")
	}
	{
		// With UppercaseEscapedNames: false
		store := Store{
			config: config.Config{
				SharedDestinationConfig: config.SharedDestinationConfig{
					UppercaseEscapedNames: false,
				},
			},
		}
		assert.Equal(t, `schema."table"`, store.ToFullyQualifiedName(tableID, true), "escaped")
		assert.Equal(t, "schema.table", store.ToFullyQualifiedName(tableID, false), "unescaped")
	}
}
