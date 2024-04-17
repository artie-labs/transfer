package bigquery

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/stretchr/testify/assert"
)

func (b *BigQueryTestSuite) TestTableRelName() {
	{
		relName, err := tableRelName("project.dataset.table")
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), "table", relName)
	}
	{
		relName, err := tableRelName("project.dataset.table.table")
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), "table.table", relName)
	}
	{
		// All the possible errors
		_, err := tableRelName("project.dataset")
		assert.ErrorContains(b.T(), err, "invalid fully qualified name: project.dataset")

		_, err = tableRelName("project")
		assert.ErrorContains(b.T(), err, "invalid fully qualified name: project")
	}
}

func TestFullyQualifiedName(t *testing.T) {
	tableID := optimization.NewTableIdentifier("database", "schema", "table")

	{
		// With UppercaseEscapedNames: true
		store := Store{
			config: config.Config{
				BigQuery: &config.BigQuery{
					ProjectID: "project",
				},
				SharedDestinationConfig: config.SharedDestinationConfig{
					UppercaseEscapedNames: true,
				},
			},
		}
		assert.Equal(t, "`project`.`database`.`TABLE`", store.ToFullyQualifiedName(tableID, true), "escaped")
		assert.Equal(t, "`project`.`database`.table", store.ToFullyQualifiedName(tableID, false), "unescaped")
	}
	{
		// With UppercaseEscapedNames: false
		store := Store{
			config: config.Config{
				BigQuery: &config.BigQuery{
					ProjectID: "project",
				},
				SharedDestinationConfig: config.SharedDestinationConfig{
					UppercaseEscapedNames: false,
				},
			},
		}
		assert.Equal(t, "`project`.`database`.`table`", store.ToFullyQualifiedName(tableID, true), "escaped")
		assert.Equal(t, "`project`.`database`.table", store.ToFullyQualifiedName(tableID, false), "unescaped")
	}
}
