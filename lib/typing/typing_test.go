package typing

import (
	"errors"
	"fmt"
	"math"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func (t *TypingTestSuite) TestNil() {
	tCfg := config.SharedTransferConfig{}
	assert.Equal(t.T(), ParseValue(tCfg, "", nil, ""), String)
	assert.Equal(t.T(), ParseValue(tCfg, "", nil, "nil"), String)
	assert.Equal(t.T(), ParseValue(tCfg, "", nil, nil), Invalid)
}

func (t *TypingTestSuite) TestJSONString() {
	type _testCase struct {
		input    string
		expected bool
	}

	testCases := []_testCase{
		{
			input:    "{}",
			expected: true,
		},
		{
			input:    `{"hello": "world"}`,
			expected: true,
		},
		{
			input: `{
				"hello": {
					"world": {
						"nested_value": true
					}
				},
				"add_a_list_here": [1, 2, 3, 4],
				"number": 7.5,
				"integerNum": 7
			}`,
			expected: true,
		},
		{
			input: `{"hello": "world"`,
		},
		{
			input: `{"hello": "world"}}`,
		},
		{
			input: `{null}`,
		},
		{
			input:    `[]`,
			expected: true,
		},
		{
			input:    `[1, 2, 3, 4]`,
			expected: true,
		},
		{
			input: `[1, 2, 3, 4`,
		},
		{
			input: ``,
		},
		{
			input: `   `,
		},
	}

	for _, tc := range testCases {
		assert.Equal(t.T(), tc.expected, IsJSON(tc.input), tc.input)
	}
}

func (t *TypingTestSuite) TestParseValueBasic() {
	stCfg := config.SharedTransferConfig{}

	// Floats
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, 7.5), Float)
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, -7.4999999), Float)
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, 7.0), Float)

	// Integers
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, 9), Integer)
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, math.MaxInt), Integer)
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, -1*math.MaxInt), Integer)

	// Invalid
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, nil), Invalid)
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, errors.New("hello")), Invalid)

	// Boolean
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, true), Boolean)
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, false), Boolean)
}

func (t *TypingTestSuite) TestParseValueArrays() {
	stCfg := config.SharedTransferConfig{}

	assert.Equal(t.T(), ParseValue(stCfg, "", nil, []string{"a", "b", "c"}), Array)
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, []interface{}{"a", 123, "c"}), Array)
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, []int64{1}), Array)
	assert.Equal(t.T(), ParseValue(stCfg, "", nil, []bool{false}), Array)
}

func (t *TypingTestSuite) TestParseValueMaps() {
	randomMaps := []interface{}{
		map[string]interface{}{
			"foo":   "bar",
			"dog":   "dusty",
			"breed": "australian shepherd",
		},
		map[string]bool{
			"foo": true,
			"bar": false,
		},
		map[int]int{
			1: 1,
			2: 2,
			3: 3,
		},
		map[string]interface{}{
			"food": map[string]interface{}{
				"pizza": "slice",
				"fruit": "apple",
			},
			"music": []string{"a", "b", "c"},
		},
	}

	for _, randomMap := range randomMaps {
		assert.Equal(t.T(), ParseValue(config.SharedTransferConfig{}, "", nil, randomMap), Struct, fmt.Sprintf("Failed message is: %v", randomMap))
	}
}

func (t *TypingTestSuite) TestDateTime() {
	// Took this list from the Go time library.
	possibleDates := []interface{}{
		"01/02 03:04:05PM '06 -0700", // The reference time, in numerical order.
		"Mon Jan 2 15:04:05 2006",
		"Mon Jan 2 15:04:05 MST 2006",
		"Mon Jan 02 15:04:05 -0700 2006",
		"02 Jan 06 15:04 MST",
		"02 Jan 06 15:04 -0700", // RFC822 with numeric zone
		"Monday, 02-Jan-06 15:04:05 MST",
		"Mon, 02 Jan 2006 15:04:05 MST",
		"Mon, 02 Jan 2006 15:04:05 -0700", // RFC1123 with numeric zone
		"2019-10-12T14:20:50.52+07:00",
	}

	for _, possibleDate := range possibleDates {
		assert.Equal(t.T(), ParseValue(config.SharedTransferConfig{}, "", nil, possibleDate).ExtendedTimeDetails.Type, ext.DateTime.Type, fmt.Sprintf("Failed format, value is: %v", possibleDate))

		// Test the parseDT function as well.
		ts, err := ext.ParseExtendedDateTime(fmt.Sprint(possibleDate), []string{})
		assert.NoError(t.T(), err, err)
		assert.False(t.T(), ts.IsZero(), ts)
	}

	ts, err := ext.ParseExtendedDateTime("random", []string{})
	assert.Error(t.T(), err, err)
	assert.Nil(t.T(), ts)
}

func (t *TypingTestSuite) TestDateTime_Fallback() {
	dtString := "Mon Jan 02 15:04:05.69944 -0700 2006"
	ts, err := ext.ParseExtendedDateTime(dtString, nil)
	assert.NoError(t.T(), err)
	assert.NotEqual(t.T(), ts.String(""), dtString)
}

func (t *TypingTestSuite) TestTime() {
	kindDetails := ParseValue(config.SharedTransferConfig{}, "", nil, "00:18:11.13116+00")
	// 00:42:26.693631Z
	assert.Equal(t.T(), ETime.Kind, kindDetails.Kind)
	assert.Equal(t.T(), ext.TimeKindType, kindDetails.ExtendedTimeDetails.Type)
}

func (t *TypingTestSuite) TestString() {
	possibleStrings := []string{
		"dusty",
		"robin",
		"abc",
	}

	for _, possibleString := range possibleStrings {
		assert.Equal(t.T(), ParseValue(config.SharedTransferConfig{}, "", nil, possibleString), String)
	}
}

func (t *TypingTestSuite) TestOptionalSchema() {
	stCfg := config.SharedTransferConfig{}

	kd := ParseValue(stCfg, "", nil, true)
	assert.Equal(t.T(), kd, Boolean)

	// Key in a nil-schema
	kd = ParseValue(stCfg, "key", nil, true)
	assert.Equal(t.T(), kd, Boolean)

	// Non-existent key in the schema.
	optionalSchema := map[string]KindDetails{
		"created_at": String,
	}

	// Parse it as a date since it doesn't exist in the optional schema.
	kd = ParseValue(stCfg, "updated_at", optionalSchema, "2023-01-01")
	assert.Equal(t.T(), ext.Date.Type, kd.ExtendedTimeDetails.Type)

	// Respecting the optional schema
	kd = ParseValue(stCfg, "created_at", optionalSchema, "2023-01-01")
	assert.Equal(t.T(), String, kd)
}
