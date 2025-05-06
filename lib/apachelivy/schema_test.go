package apachelivy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSchemaResponse_BuildColumns(t *testing.T) {
	{
		// Test case #1 - No columns
		resp := GetSchemaResponse{
			Schema: GetSchemaStructResponse{
				Fields: []GetSchemaFieldResponse{},
			},
			Data: [][]any{},
		}

		_, err := resp.BuildColumns()
		assert.ErrorContains(t, err, "col_name, data_type, or comment not found")
	}
	{
		// Test case #2 - With columns
		resp := GetSchemaResponse{
			Schema: GetSchemaStructResponse{
				Fields: []GetSchemaFieldResponse{
					{
						Name:     "col_name",
						Type:     "STRING",
						Nullable: false,
					},
					{
						Name:     "data_type",
						Type:     "STRING",
						Nullable: false,
					},
					{
						Name:     "comment",
						Type:     "STRING",
						Nullable: true,
					},
				},
			},
			Data: [][]any{
				{
					"id",
					"bigint",
					"",
				},
				{
					"first_name",
					"string",
					"",
				},
				{
					"last_name",
					"string",
					"",
				},
				{
					"email",
					"string",
					"",
				},
			},
		}

		cols, err := resp.BuildColumns()
		assert.NoError(t, err)
		assert.Equal(t, 4, len(cols))

		assert.Equal(t, "id", cols[0].Name)
		assert.Equal(t, "bigint", cols[0].DataType)
		assert.Equal(t, "", cols[0].Comment)

		assert.Equal(t, "first_name", cols[1].Name)
		assert.Equal(t, "string", cols[1].DataType)
		assert.Equal(t, "", cols[1].Comment)

		assert.Equal(t, "last_name", cols[2].Name)
		assert.Equal(t, "string", cols[2].DataType)
		assert.Equal(t, "", cols[2].Comment)

		assert.Equal(t, "email", cols[3].Name)
		assert.Equal(t, "string", cols[3].DataType)
		assert.Equal(t, "", cols[3].Comment)
	}
}
