package typing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumns_Add_Duplicate(t *testing.T) {
	var cols Columns
	duplicateColumns := []Column{{Name: "foo"}, {Name: "foo"}, {Name: "foo"}, {Name: "foo"}, {Name: "foo"}, {Name: "foo"}}
	for _, duplicateColumn := range duplicateColumns {
		cols.AddColumn(duplicateColumn)
	}

	assert.Equal(t, len(cols.GetColumns()), 1, "AddColumn() de-duplicates")
}

func TestColumns_Mutation(t *testing.T) {
	var cols Columns
	colsToAdd := []Column{{Name: "foo", KindDetails: String}, {Name: "bar", KindDetails: Struct}}
	// Insert
	for _, colToAdd := range colsToAdd {
		cols.AddColumn(colToAdd)
	}

	assert.Equal(t, len(cols.GetColumns()), 2)
	fooCol, isOk := cols.GetColumn("foo")
	assert.True(t, isOk)
	assert.Equal(t, String, fooCol.KindDetails)

	barCol, isOk := cols.GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, Struct, barCol.KindDetails)

	// Update
	cols.UpdateColumn(Column{
		Name:        "foo",
		KindDetails: Integer,
	})

	cols.UpdateColumn(Column{
		Name:        "bar",
		KindDetails: Boolean,
	})

	fooCol, isOk = cols.GetColumn("foo")
	assert.True(t, isOk)
	assert.Equal(t, Integer, fooCol.KindDetails)

	barCol, isOk = cols.GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, Boolean, barCol.KindDetails)

	// Delete
	cols.DeleteColumn("foo")
	assert.Equal(t, len(cols.GetColumns()), 1)
	cols.DeleteColumn("bar")
	assert.Equal(t, len(cols.GetColumns()), 0)
}
