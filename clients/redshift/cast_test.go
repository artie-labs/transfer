package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (r *RedshiftTestSuite) TestCanIncreasePrecision() {
	{
		// Not a string
		assert.False(r.T(), canIncreasePrecision(typing.Struct))
	}
	{
		// String, but precision is not specified
		assert.False(r.T(), canIncreasePrecision(typing.String))
	}
	{
		// String, but maxed out already
		assert.False(r.T(), canIncreasePrecision(
			typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: typing.ToPtr(maxRedshiftLength),
			}),
		)
	}
	{
		// String, precision is low and can be increased
		assert.True(r.T(), canIncreasePrecision(
			typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: typing.ToPtr(maxRedshiftLength - 1),
			}),
		)
	}
}

func (r *RedshiftTestSuite) TestReplaceExceededValues() {
	{
		// Irrelevant data type
		{
			// Integer
			assert.Equal(r.T(), "123", replaceExceededValues("123", typing.Integer, false))
		}
		{
			// Returns the full value since it's not a struct or string
			// This is invalid and should not happen, but it's here to ensure we're only checking for structs and strings.
			value := stringutil.Random(int(maxRedshiftLength + 1))
			assert.Equal(r.T(), value, replaceExceededValues(value, typing.Integer, false))
		}
	}
	{
		// Exceeded
		{
			// String
			{
				// TruncateExceededValue = false
				assert.Equal(r.T(), constants.ExceededValueMarker, replaceExceededValues(stringutil.Random(int(maxRedshiftLength)+1), typing.String, false))
			}
			{
				// TruncateExceededValue = false, string precision specified
				stringKd := typing.KindDetails{
					Kind:                    typing.String.Kind,
					OptionalStringPrecision: typing.ToPtr(int32(3)),
				}

				assert.Equal(r.T(), constants.ExceededValueMarker, replaceExceededValues("hello", stringKd, false))
			}
			{
				// TruncateExceededValue = true
				superLongString := stringutil.Random(int(maxRedshiftLength) + 1)
				assert.Equal(r.T(), superLongString[:maxRedshiftLength], replaceExceededValues(superLongString, typing.String, true))
			}
			{
				// TruncateExceededValue = true, string precision specified
				stringKd := typing.KindDetails{
					Kind:                    typing.String.Kind,
					OptionalStringPrecision: typing.ToPtr(int32(3)),
				}

				assert.Equal(r.T(), "hel", replaceExceededValues("hello", stringKd, true))
			}
		}
		{
			// Struct and masked
			assert.Equal(r.T(), fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker), replaceExceededValues(fmt.Sprintf(`{"foo": "%s"}`, stringutil.Random(int(maxRedshiftLength)+1)), typing.Struct, false))
		}
	}
	{
		// Valid
		{
			// Not masked
			assert.Equal(r.T(), `{"foo": "bar"}`, replaceExceededValues(`{"foo": "bar"}`, typing.Struct, false))
			assert.Equal(r.T(), "hello world", replaceExceededValues("hello world", typing.String, false))
		}
	}
}

func (r *RedshiftTestSuite) TestCastColValStaging() {
	{
		// Exceeded
		{
			// String
			{
				// TruncateExceededValue = false
				value, err := castColValStaging(stringutil.Random(int(maxRedshiftLength)+1), typing.String, false)
				assert.NoError(r.T(), err)
				assert.Equal(r.T(), constants.ExceededValueMarker, value)
			}
			{
				// TruncateExceededValue = true
				value := stringutil.Random(int(maxRedshiftLength) + 1)
				value, err := castColValStaging(value, typing.String, true)
				assert.NoError(r.T(), err)
				assert.Equal(r.T(), value[:maxRedshiftLength], value)
			}
		}
		{
			// Masked struct
			value, err := castColValStaging(fmt.Sprintf(`{"foo": "%s"}`, stringutil.Random(int(maxRedshiftLength)+1)), typing.Struct, false)
			assert.NoError(r.T(), err)
			assert.Equal(r.T(), fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker), value)
		}
	}
	{
		// Not exceeded
		{
			// Valid string
			value, err := castColValStaging("thisissuperlongbutnotlongenoughtogetmasked", typing.String, false)
			assert.NoError(r.T(), err)
			assert.Equal(r.T(), "thisissuperlongbutnotlongenoughtogetmasked", value)
		}
		{
			// Valid struct
			value, err := castColValStaging(`{"foo": "bar"}`, typing.Struct, false)
			assert.NoError(r.T(), err)
			assert.Equal(r.T(), `{"foo": "bar"}`, value)
		}
	}
}
