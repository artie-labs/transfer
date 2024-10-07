package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (r *RedshiftTestSuite) TestReplaceExceededValues() {
	{
		// expandStringPrecision = false
		{
			// Irrelevant data type
			{
				// Integer
				result := replaceExceededValues("123", typing.Integer, false, false)
				assert.Equal(r.T(), "123", result.Value)
				assert.Zero(r.T(), result.NewLength)
			}
			{
				// Returns the full value since it's not a struct or string
				// This is invalid and should not happen, but it's here to ensure we're only checking for structs and strings.
				value := stringutil.Random(int(maxRedshiftLength + 1))
				result := replaceExceededValues(value, typing.Integer, false, false)
				assert.Equal(r.T(), value, result.Value)
				assert.Zero(r.T(), result.NewLength)
			}
		}
		{
			// Exceeded
			{
				// String
				{
					// TruncateExceededValue = false
					result := replaceExceededValues(stringutil.Random(int(maxRedshiftLength)+1), typing.String, false, false)
					assert.Equal(r.T(), constants.ExceededValueMarker, result.Value)
					assert.Zero(r.T(), result.NewLength)
				}
				{
					// TruncateExceededValue = false, string precision specified
					stringKd := typing.KindDetails{
						Kind:                    typing.String.Kind,
						OptionalStringPrecision: typing.ToPtr(int32(3)),
					}

					result := replaceExceededValues("hello", stringKd, false, false)
					assert.Equal(r.T(), constants.ExceededValueMarker, result.Value)
					assert.Zero(r.T(), result.NewLength)
				}
				{
					// TruncateExceededValue = true
					superLongString := stringutil.Random(int(maxRedshiftLength) + 1)
					result := replaceExceededValues(superLongString, typing.String, true, false)
					assert.Equal(r.T(), superLongString[:maxRedshiftLength], result.Value)
					assert.Zero(r.T(), result.NewLength)
				}
				{
					// TruncateExceededValue = true, string precision specified
					stringKd := typing.KindDetails{
						Kind:                    typing.String.Kind,
						OptionalStringPrecision: typing.ToPtr(int32(3)),
					}

					result := replaceExceededValues("hello", stringKd, true, false)
					assert.Equal(r.T(), "hel", result.Value)
					assert.Zero(r.T(), result.NewLength)
				}
			}
			{
				// Struct and masked
				result := replaceExceededValues(fmt.Sprintf(`{"foo": "%s"}`, stringutil.Random(int(maxRedshiftLength)+1)), typing.Struct, false, false)
				assert.Equal(r.T(), fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker), result.Value)
				assert.Zero(r.T(), result.NewLength)
			}
		}
		{
			// Valid
			{
				// Not masked
				{
					result := replaceExceededValues(`{"foo": "bar"}`, typing.Struct, false, false)
					assert.Equal(r.T(), `{"foo": "bar"}`, result.Value)
					assert.Zero(r.T(), result.NewLength)
				}
				{
					result := replaceExceededValues("hello world", typing.String, false, false)
					assert.Equal(r.T(), "hello world", result.Value)
					assert.Zero(r.T(), result.NewLength)
				}
			}
		}
	}
}

func (r *RedshiftTestSuite) TestCastColValStaging() {
	{
		// nil
		{
			// Struct
			result, err := castColValStaging(nil, typing.Struct, false, false)
			assert.NoError(r.T(), err)
			assert.Empty(r.T(), result.Value)
		}
		{
			// Not struct
			result, err := castColValStaging(nil, typing.String, false, false)
			assert.NoError(r.T(), err)
			assert.Equal(r.T(), `\N`, result.Value)
		}
	}
}
