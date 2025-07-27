package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config"
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
			expectedMap := map[string]string{
				stringutil.Random(int(maxStringLength + 1)):
			}

			{
				// Integer
				result := replaceExceededValues("123", typing.Integer, false, false)
				assert.Equal(r.T(), "123", result.Value)
				assert.Zero(r.T(), result.NewLength)
			}
			{
				// Returns the full value since it's not a struct or string
				// This is invalid and should not happen, but it's here to ensure we're only checking for structs and strings.
				value := stringutil.Random(int(maxStringLength + 1))
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
					result := replaceExceededValues(stringutil.Random(int(maxStringLength)+1), typing.String, false, false)
					assert.Equal(r.T(), constants.ExceededValueMarker, result.Value)
					assert.True(r.T(), result.Exceeded)
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
					assert.True(r.T(), result.Exceeded)
					assert.Zero(r.T(), result.NewLength)
				}
				{
					// TruncateExceededValue = true
					superLongString := stringutil.Random(int(maxStringLength) + 1)
					result := replaceExceededValues(superLongString, typing.String, true, false)
					assert.Equal(r.T(), superLongString[:maxStringLength], result.Value)
					assert.Zero(r.T(), result.NewLength)
					assert.True(r.T(), result.Exceeded)
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
					assert.True(r.T(), result.Exceeded)
				}
			}
			{
				// Super
				{
					// Masked (data type is a JSON object)
					result := replaceExceededValues(fmt.Sprintf(`{"foo": "%s"}`, stringutil.Random(int(maxSuperLength)+1)), typing.Struct, false, false)
					assert.Equal(r.T(), fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker), result.Value)
					assert.Zero(r.T(), result.NewLength)
					assert.True(r.T(), result.Exceeded)
				}
				{
					// Masked (data type is an array)
					result := replaceExceededValues(fmt.Sprintf(`["%s"]`, stringutil.Random(int(maxSuperLength)+1)), typing.Struct, false, false)
					assert.Equal(r.T(), fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker), result.Value)
					assert.Zero(r.T(), result.NewLength)
					assert.True(r.T(), result.Exceeded)
				}
				{
					// Masked (data type is a string)
					result := replaceExceededValues(stringutil.Random(int(maxStringLength)+1), typing.Struct, false, false)
					assert.Equal(r.T(), fmt.Sprintf(`"%s"`, constants.ExceededValueMarker), result.Value)
					assert.Zero(r.T(), result.NewLength)
					assert.True(r.T(), result.Exceeded)
				}
			}
		}
		{
			// Valid
			{
				// Not masked
				{
					// String
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
				{
					// SUPER
					{
						// Value is a struct
						value := fmt.Sprintf(`{"foo": "%s"}`, stringutil.Random(int(maxSuperLength)-11))
						result := replaceExceededValues(value, typing.Struct, false, false)
						assert.Equal(r.T(), value, result.Value)
						assert.Zero(r.T(), result.NewLength)
						assert.False(r.T(), result.Exceeded)
					}
					{
						// Value is an array
						value := fmt.Sprintf(`["%s"]`, stringutil.Random(int(maxSuperLength)-11))
						result := replaceExceededValues(value, typing.Struct, false, false)
						assert.Equal(r.T(), value, result.Value)
						assert.Zero(r.T(), result.NewLength)
						assert.False(r.T(), result.Exceeded)
					}
					{
						// Value is a string
						result := replaceExceededValues("hello world", typing.Struct, false, false)
						assert.Equal(r.T(), "hello world", result.Value)
						assert.Zero(r.T(), result.NewLength)
						assert.False(r.T(), result.Exceeded)
					}
				}
			}
		}
	}
	{
		// expandStringPrecision = true
		{
			// Irrelevant data type
			{
				// Integer
				result := replaceExceededValues("123", typing.Integer, false, true)
				assert.Equal(r.T(), "123", result.Value)
				assert.Zero(r.T(), result.NewLength)
			}
			{
				// Returns the full value since it's not a struct or string
				// This is invalid and should not happen, but it's here to ensure we're only checking for structs and strings.
				value := stringutil.Random(int(maxStringLength + 1))
				result := replaceExceededValues(value, typing.Integer, false, true)
				assert.Equal(r.T(), value, result.Value)
				assert.Zero(r.T(), result.NewLength)
			}
		}
		{
			// Exceeded the column string precision but not Redshift's max length
			{
				stringKd := typing.KindDetails{
					Kind:                    typing.String.Kind,
					OptionalStringPrecision: typing.ToPtr(int32(3)),
				}

				result := replaceExceededValues("hello", stringKd, true, true)
				assert.Equal(r.T(), "hello", result.Value)
				assert.Equal(r.T(), int32(5), result.NewLength)
			}
		}
		{
			// Exceeded both column and Redshift precision, so the value got replaced with an exceeded placeholder.
			{
				stringKd := typing.KindDetails{
					Kind:                    typing.String.Kind,
					OptionalStringPrecision: typing.ToPtr(maxStringLength),
				}

				superLongString := stringutil.Random(int(maxStringLength) + 1)
				result := replaceExceededValues(superLongString, stringKd, false, true)
				assert.Equal(r.T(), constants.ExceededValueMarker, result.Value)
				assert.Zero(r.T(), result.NewLength)
			}
		}
	}
}

func (r *RedshiftTestSuite) TestCastColValStaging() {
	settings := config.SharedDestinationSettings{}
	{
		// nil
		{
			// Struct
			result, err := castColValStaging(nil, typing.Struct, settings)
			assert.NoError(r.T(), err)
			assert.Empty(r.T(), result.Value)
		}
		{
			// Not struct
			result, err := castColValStaging(nil, typing.String, settings)
			assert.NoError(r.T(), err)
			assert.Equal(r.T(), constants.NullValuePlaceholder, result.Value)
		}
	}
}
