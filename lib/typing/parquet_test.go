package typing

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go/parquet"
)

func TestKindDetails_ParquetAnnotation(t *testing.T) {
	{
		// String field
		for _, kd := range []KindDetails{String, Struct} {
			field, err := kd.ParquetAnnotation("foo", nil)
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
		field, err := Integer.ParquetAnnotation("foo", nil)
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
	{
		// Time
		field, err := Time.ParquetAnnotation("foo", nil)
		assert.NoError(t, err)
		assert.Equal(t,
			Field{
				Tag: FieldTag{
					Name:          "foo",
					Type:          ToPtr(parquet.Type_INT32.String()),
					ConvertedType: ToPtr(parquet.ConvertedType_TIME_MILLIS.String()),
				}.String(),
			},
			*field,
		)
	}
	{
		// Date
		field, err := Date.ParquetAnnotation("foo", nil)
		assert.NoError(t, err)
		assert.Equal(t,
			Field{
				Tag: FieldTag{
					Name:          "foo",
					Type:          ToPtr(parquet.Type_INT32.String()),
					ConvertedType: ToPtr(parquet.ConvertedType_DATE.String()),
				}.String(),
			},
			*field,
		)
	}
	{
		// Timestamps
		{
			// No location
			for _, kd := range []KindDetails{TimestampTZ, TimestampNTZ} {
				field, err := kd.ParquetAnnotation("foo", nil)
				assert.NoError(t, err)
				assert.Equal(t,
					Field{
						Tag: FieldTag{
							Name:             "foo",
							Type:             ToPtr(parquet.Type_INT64.String()),
							LogicalType:      ToPtr("TIMESTAMP"),
							IsAdjustedForUTC: ToPtr(true),
							Unit:             ToPtr("MILLIS"),
						}.String(),
					},
					*field,
				)
			}
		}
		{
			// With location
			est, err := time.LoadLocation("America/New_York")
			assert.NoError(t, err)

			for _, kd := range []KindDetails{TimestampTZ, TimestampNTZ} {
				field, err := kd.ParquetAnnotation("foo", est)
				assert.NoError(t, err)
				assert.Equal(t,
					Field{
						Tag: FieldTag{
							Name:             "foo",
							Type:             ToPtr(parquet.Type_INT64.String()),
							LogicalType:      ToPtr("TIMESTAMP"),
							IsAdjustedForUTC: ToPtr(false),
							Unit:             ToPtr("MILLIS"),
						}.String(),
					},
					*field,
				)
			}
		}
	}
}
