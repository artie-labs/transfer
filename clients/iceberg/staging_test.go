package iceberg

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func TestBuildColumnParts(t *testing.T) {
	store := Store{}

	{
		// Basic column types
		columns := []columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("name", typing.String),
			columns.NewColumn("is_active", typing.Boolean),
		}
		expected := []string{
			"id LONG",
			"name STRING",
			"is_active BOOLEAN",
		}
		result, err := store.buildColumnParts(columns)
		assert.NoError(t, err)
		assert.Equal(t, expected, result, "Basic column types should be properly formatted")
	}
	{
		// With primary key
		columns := []columns.Column{
			func() columns.Column {
				col := columns.NewColumn("user_id", typing.Integer)
				col.SetPrimaryKeyForTest(true)
				return col
			}(),
			columns.NewColumn("email", typing.String),
		}
		expected := []string{
			"user_id LONG",
			"email STRING",
		}
		result, err := store.buildColumnParts(columns)
		assert.NoError(t, err)
		assert.Equal(t, expected, result, "Primary key columns should be properly formatted")
	}
	{
		// Complex types
		columns := []columns.Column{
			columns.NewColumn("metadata", typing.Struct),
			columns.NewColumn("tags", typing.Array),
			columns.NewColumn("created_at", typing.TimestampNTZ),
		}
		expected := []string{
			"metadata STRING",
			"tags STRING",
			"created_at TIMESTAMP_NTZ",
		}
		result, err := store.buildColumnParts(columns)
		assert.NoError(t, err)
		assert.Equal(t, expected, result, "Complex types should be properly formatted")
	}
	{
		// Empty columns
		columns := []columns.Column{}
		expected := []string(nil)
		result, err := store.buildColumnParts(columns)
		assert.NoError(t, err)
		assert.Equal(t, expected, result, "Empty columns should return empty slice")
	}
	{
		// Special characters in column names
		columns := []columns.Column{
			columns.NewColumn("user.name", typing.String),
			columns.NewColumn("order_id", typing.Integer),
		}
		expected := []string{
			"user.name STRING",
			"order_id LONG",
		}
		result, err := store.buildColumnParts(columns)
		assert.NoError(t, err)
		assert.Equal(t, expected, result, "Special characters in column names should be properly quoted")
	}
	{
		// Mixed case column names
		columns := []columns.Column{
			columns.NewColumn("UserID", typing.Integer),
			columns.NewColumn("firstName", typing.String),
		}
		expected := []string{
			"userid LONG",
			"firstname STRING",
		}
		result, err := store.buildColumnParts(columns)
		assert.NoError(t, err)
		assert.Equal(t, expected, result, "Mixed case column names should be preserved")
	}
	{
		// Numeric column names
		columns := []columns.Column{
			columns.NewColumn("123_id", typing.Integer),
			columns.NewColumn("456_name", typing.String),
		}
		expected := []string{
			"123_id LONG",
			"456_name STRING",
		}
		result, err := store.buildColumnParts(columns)
		assert.NoError(t, err)
		assert.Equal(t, expected, result, "Numeric column names should be properly quoted")
	}
	{
		// Nil columns slice
		result, err := store.buildColumnParts(nil)
		assert.NoError(t, err)
		assert.Empty(t, result, "Nil columns should return empty slice")
	}
	{
		// Single column with empty name
		columns := []columns.Column{
			columns.NewColumn("", typing.String),
		}
		expected := []string{" STRING"}
		result, err := store.buildColumnParts(columns)
		assert.NoError(t, err)
		assert.Equal(t, expected, result, "Empty column name should be properly quoted")
	}
	{
		// Column with maximum length name
		longName := "a" + string(make([]byte, 1000)) // Create a very long name
		columns := []columns.Column{
			columns.NewColumn(longName, typing.String),
		}
		expected := []string{fmt.Sprintf("%s STRING", longName)}
		result, err := store.buildColumnParts(columns)
		assert.NoError(t, err)
		assert.Equal(t, expected, result, "Long column name should be properly quoted")
	}
}
