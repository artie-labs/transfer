package sql

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestMSSQLDialect_KindForDataType(t *testing.T) {
	dialect := MSSQLDialect{}

	colToExpectedKind := map[string]typing.KindDetails{
		"char":      typing.String,
		"varchar":   typing.String,
		"nchar":     typing.String,
		"nvarchar":  typing.String,
		"ntext":     typing.String,
		"text":      typing.String,
		"smallint":  typing.Integer,
		"tinyint":   typing.Integer,
		"int":       typing.Integer,
		"float":     typing.Float,
		"real":      typing.Float,
		"bit":       typing.Boolean,
		"date":      typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType),
		"time":      typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType),
		"datetime":  typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
		"datetime2": typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType),
	}

	for col, expectedKind := range colToExpectedKind {
		kd, err := dialect.KindForDataType(col, "")
		assert.NoError(t, err)
		assert.Equal(t, expectedKind.Kind, kd.Kind, col)
	}

	{
		_, err := dialect.KindForDataType("numeric(5", "")
		assert.ErrorContains(t, err, "missing closing parenthesis")
	}
	{
		kd, err := dialect.KindForDataType("numeric(5, 2)", "")
		assert.NoError(t, err)
		assert.Equal(t, typing.EDecimal.Kind, kd.Kind)
		assert.Equal(t, 5, *kd.ExtendedDecimalDetails.Precision())
		assert.Equal(t, 2, kd.ExtendedDecimalDetails.Scale())
	}
	{
		kd, err := dialect.KindForDataType("char", "5")
		assert.NoError(t, err)
		assert.Equal(t, typing.String.Kind, kd.Kind)
		assert.Equal(t, 5, *kd.OptionalStringPrecision)
	}
}

func TestMSSQLDialect_BuildCreateTableQuery(t *testing.T) {
	// Temporary:
	assert.Equal(t,
		`CREATE TABLE {TABLE} ({PART_1},{PART_2});`,
		MSSQLDialect{}.BuildCreateTableQuery("{TABLE}", true, []string{"{PART_1}", "{PART_2}"}),
	)
	// Not temporary:
	assert.Equal(t,
		`CREATE TABLE {TABLE} ({PART_1},{PART_2});`,
		MSSQLDialect{}.BuildCreateTableQuery("{TABLE}", false, []string{"{PART_1}", "{PART_2}"}),
	)
}
