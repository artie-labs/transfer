package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func (r *RedshiftTestSuite) TestReplaceExceededValues() {
	{
		// Masked, reached the DDL limit
		assert.Equal(r.T(), constants.ExceededValueMarker, replaceExceededValues(stringutil.Random(int(maxRedshiftLength)+1), typing.String))
	}
	{
		// Masked, reached the string precision limit
		stringKd := typing.KindDetails{
			Kind:                    typing.String.Kind,
			OptionalStringPrecision: ptr.ToInt32(3),
		}

		assert.Equal(r.T(), constants.ExceededValueMarker, replaceExceededValues("hello", stringKd))
	}
	{
		// Struct and masked
		assert.Equal(r.T(), fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker), replaceExceededValues(fmt.Sprintf(`{"foo": "%s"}`, stringutil.Random(int(maxRedshiftLength)+1)), typing.Struct))
	}
	{
		// Not masked
		assert.Equal(r.T(), `{"foo": "bar"}`, replaceExceededValues(`{"foo": "bar"}`, typing.Struct))
		assert.Equal(r.T(), "hello world", replaceExceededValues("hello world", typing.String))
	}
}

func (r *RedshiftTestSuite) TestCastColValStaging() {
	{
		// Masked
		value, err := castColValStaging(stringutil.Random(int(maxRedshiftLength)+1), typing.String)
		assert.NoError(r.T(), err)
		assert.Equal(r.T(), constants.ExceededValueMarker, value)
	}
	{
		// Valid
		value, err := castColValStaging("thisissuperlongbutnotlongenoughtogetmasked", typing.String)
		assert.NoError(r.T(), err)
		assert.Equal(r.T(), "thisissuperlongbutnotlongenoughtogetmasked", value)
	}
	{
		// Masked struct
		value, err := castColValStaging(fmt.Sprintf(`{"foo": "%s"}`, stringutil.Random(int(maxRedshiftLength)+1)), typing.Struct)
		assert.NoError(r.T(), err)
		assert.Equal(r.T(), fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker), value)
	}
	{
		// Valid struct
		value, err := castColValStaging(`{"foo": "bar"}`, typing.Struct)
		assert.NoError(r.T(), err)
		assert.Equal(r.T(), `{"foo": "bar"}`, value)
	}
}
