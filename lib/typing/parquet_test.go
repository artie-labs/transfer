package typing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKindDetails_ParquetAnnotation(t *testing.T) {
	{
		// String field
		for _, kd := range []KindDetails{String, Struct, Date, Time} {
			field, err := kd.ParquetAnnotation("foo")
			assert.NoError(t, err)
			assert.Equal(t,
				Field{
					Tag: FieldTag{
						Name:          "foo",
						InName:        ToPtr("foo"),
						Type:          ToPtr("BYTE_ARRAY"),
						ConvertedType: ToPtr("UTF8"),
					}.String(),
				},
				*field,
			)
		}
	}
	{
		// Integers
		for _, kd := range []KindDetails{Integer, TimestampTZ, TimestampNTZ} {
			field, err := kd.ParquetAnnotation("foo")
			assert.NoError(t, err)
			assert.Equal(t,
				Field{
					Tag: FieldTag{
						Name:   "foo",
						InName: ToPtr("foo"),
						Type:   ToPtr("INT64"),
					}.String(),
				},
				*field,
			)
		}
	}
}
