package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestColumnsMatch(t *testing.T) {
	{
		// Both empty
		assert.True(t, columnsMatch(nil, nil))
		assert.True(t, columnsMatch([]columns.Column{}, []columns.Column{}))
	}
	{
		// Same columns, same order
		a := []columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("name", typing.String),
		}
		b := []columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("name", typing.String),
		}
		assert.True(t, columnsMatch(a, b))
	}
	{
		// Different lengths
		a := []columns.Column{
			columns.NewColumn("id", typing.Integer),
		}
		b := []columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("name", typing.String),
		}
		assert.False(t, columnsMatch(a, b))
	}
	{
		// Same length, different names
		a := []columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("name", typing.String),
		}
		b := []columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("email", typing.String),
		}
		assert.False(t, columnsMatch(a, b))
	}
	{
		// Same names, different types
		a := []columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("value", typing.String),
		}
		b := []columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("value", typing.Float),
		}
		assert.False(t, columnsMatch(a, b))
	}
	{
		// Same names and types, different order
		a := []columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("name", typing.String),
		}
		b := []columns.Column{
			columns.NewColumn("name", typing.String),
			columns.NewColumn("id", typing.Integer),
		}
		assert.False(t, columnsMatch(a, b))
	}
	{
		// One nil, one non-empty
		a := []columns.Column{
			columns.NewColumn("id", typing.Integer),
		}
		assert.False(t, columnsMatch(nil, a))
		assert.False(t, columnsMatch(a, nil))
	}
}
