package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (r *RedshiftTestSuite) TestCanIncreasePrecision() {
	{
		// False
		{
			// Not a string
			assert.False(r.T(), canIncreasePrecision(typing.Struct, 123))
		}
		{
			// String, but precision not specified.
			assert.False(r.T(), canIncreasePrecision(typing.String, 123))
		}
		{
			// String and precision specified, but value length exceeds maxRedshiftLength
			assert.False(r.T(), canIncreasePrecision(
				typing.KindDetails{
					Kind:                    typing.String.Kind,
					OptionalStringPrecision: typing.ToPtr(int32(123)),
				},
				maxRedshiftLength+1),
			)
		}
	}
	{
		// True
		{
			// String, precision is low and can be increased
			assert.True(r.T(), canIncreasePrecision(
				typing.KindDetails{
					Kind:                    typing.String.Kind,
					OptionalStringPrecision: typing.ToPtr(int32(123)),
				},
				123),
			)
		}
	}
}

func (r *RedshiftTestSuite) TestReplaceExceededValues() {
	{
		// Irrelevant data type
		{
			// Integer

			value, _ := replaceExceededValues("123", typing.Integer, false, false)
			assert.Equal(r.T(), "123", value)
		}
		{
			// Returns the full value since it's not a struct or string
			// This is invalid and should not happen, but it's here to ensure we're only checking for structs and strings.
			input := stringutil.Random(int(maxRedshiftLength + 1))
			value, _ := replaceExceededValues(input, typing.Integer, false, false)
			assert.Equal(r.T(), input, value)
		}
	}
	{
		// Exceeded
		{
			{
				// TruncateExceededValue = false, IncreaseStringPrecision = false
				value, shouldIncrease := replaceExceededValues(stringutil.Random(int(maxRedshiftLength)+1), typing.String, false, false)
				assert.Equal(r.T(), constants.ExceededValueMarker, value)
				assert.False(r.T(), shouldIncrease)
			}
			{
				// TruncateExceededValue = false, string precision specified, IncreaseStringPrecision = false
				stringKd := typing.KindDetails{
					Kind:                    typing.String.Kind,
					OptionalStringPrecision: typing.ToPtr(int32(3)),
				}
				value, shouldIncrease := replaceExceededValues("hello", stringKd, false, false)
				assert.Equal(r.T(), constants.ExceededValueMarker, value)
				assert.False(r.T(), shouldIncrease)
			}
			{
				// TruncateExceededValue = true, IncreaseStringPrecision = false
				input := stringutil.Random(int(maxRedshiftLength) + 1)
				value, shouldIncrease := replaceExceededValues(input, typing.String, true, false)
				assert.Equal(r.T(), input[:maxRedshiftLength], value)
				assert.False(r.T(), shouldIncrease)
			}
			{
				// TruncateExceededValue = true, string precision specified, IncreaseStringPrecision = false
				stringKd := typing.KindDetails{
					Kind:                    typing.String.Kind,
					OptionalStringPrecision: typing.ToPtr(int32(3)),
				}
				value, shouldIncrease := replaceExceededValues("hello", stringKd, true, false)
				assert.Equal(r.T(), "hel", value)
				assert.False(r.T(), shouldIncrease)
			}
		}
		{
			// Struct and masked
			value, shouldIncrease := replaceExceededValues(fmt.Sprintf(`{"foo": "%s"}`, stringutil.Random(int(maxRedshiftLength)+1)), typing.Struct, false, false)
			assert.Equal(r.T(), fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker), value)
			assert.False(r.T(), shouldIncrease)
		}
	}
	{
		// Valid
		{
			// Not masked
			{
				value, shouldIncrease := replaceExceededValues(`{"foo": "bar"}`, typing.Struct, false, false)
				assert.Equal(r.T(), `{"foo": "bar"}`, value)
				assert.False(r.T(), shouldIncrease)
			}
			{
				value, shouldIncrease := replaceExceededValues("hello world", typing.String, false, false)
				assert.Equal(r.T(), "hello world", value)
				assert.False(r.T(), shouldIncrease)
			}
		}
	}
}

func (r *RedshiftTestSuite) TestCastColValStaging() {
	{
		// Exceeded
		{
			// String
			{
				// TruncateExceededValue = false, IncreaseStringPrecision = false
				value, shouldIncrease, err := castColValStaging(stringutil.Random(int(maxRedshiftLength)+1), typing.String, false, false)
				assert.NoError(r.T(), err)
				assert.Equal(r.T(), constants.ExceededValueMarker, value)
				assert.False(r.T(), shouldIncrease)
			}
			{
				// TruncateExceededValue = true, IncreaseStringPrecision = false
				input := stringutil.Random(int(maxRedshiftLength) + 1)
				value, shouldIncrease, err := castColValStaging(input, typing.String, true, false)
				assert.NoError(r.T(), err)
				assert.Equal(r.T(), input[:maxRedshiftLength], value)
				assert.False(r.T(), shouldIncrease)
			}
			{
				// TruncateExceededValue = false, IncreaseStringPrecision = true
				stringKd := typing.KindDetails{
					Kind:                    typing.String.Kind,
					OptionalStringPrecision: typing.ToPtr(int32(3)),
				}

				value, shouldIncrease, err := castColValStaging("hello", stringKd, false, true)
				assert.NoError(r.T(), err)
				assert.Equal(r.T(), "hello", value)
				assert.True(r.T(), shouldIncrease)
			}
			{
				// TruncateExceededValue = true, IncreaseStringPrecision = true
				input := stringutil.Random(int(maxRedshiftLength) + 1)
				stringPrecision := int32(3)
				stringKd := typing.KindDetails{
					Kind:                    typing.String.Kind,
					OptionalStringPrecision: typing.ToPtr(stringPrecision),
				}

				value, shouldIncrease, err := castColValStaging(input, stringKd, true, true)
				assert.NoError(r.T(), err)
				assert.Equal(r.T(), input[:stringPrecision], value)
				assert.False(r.T(), shouldIncrease)
			}
		}
		{
			// Masked struct
			value, shouldIncrease, err := castColValStaging(fmt.Sprintf(`{"foo": "%s"}`, stringutil.Random(int(maxRedshiftLength)+1)), typing.Struct, false, false)
			assert.NoError(r.T(), err)
			assert.Equal(r.T(), fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker), value)
			assert.False(r.T(), shouldIncrease)
		}
	}
	{
		// Not exceeded
		{
			// Valid string
			value, shouldIncrease, err := castColValStaging("thisissuperlongbutnotlongenoughtogetmasked", typing.String, false, false)
			assert.NoError(r.T(), err)
			assert.Equal(r.T(), "thisissuperlongbutnotlongenoughtogetmasked", value)
			assert.False(r.T(), shouldIncrease)
		}
		{
			// Valid struct
			value, shouldIncrease, err := castColValStaging(`{"foo": "bar"}`, typing.Struct, false, false)
			assert.NoError(r.T(), err)
			assert.Equal(r.T(), `{"foo": "bar"}`, value)
			assert.False(r.T(), shouldIncrease)
		}
	}
}
