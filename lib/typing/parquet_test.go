package typing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go/parquet"
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
		for _, kd := range []KindDetails{Integer} {
			field, err := kd.ParquetAnnotation("foo")
			assert.NoError(t, err)
			assert.Equal(t,
				Field{
					Tag: FieldTag{
						Name: "foo",
						Type: ToPtr(parquet.Type_INT64.String()),
					}.String(),
				},
				*field,
			)
		}
	}
	{
		// Timestamps
		for _, kd := range []KindDetails{TimestampTZ, TimestampNTZ} {
			field, err := kd.ParquetAnnotation("foo")
			assert.NoError(t, err)
			assert.Equal(t,
				Field{
					Tag: FieldTag{
						Name:          "foo",
						Type:          ToPtr(parquet.Type_INT64.String()),
						ConvertedType: ToPtr(parquet.ConvertedType_TIMESTAMP_MILLIS.String()),
					}.String(),
				},
				*field,
			)
		}
	}
}
