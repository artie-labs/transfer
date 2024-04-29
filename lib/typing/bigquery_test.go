package typing

import (
	"fmt"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestBigQueryTypeToKind(t *testing.T) {
	bqColToExpectedKind := map[string]KindDetails{
		//// Number
		"numeric":           EDecimal,
		"numeric(5)":        Integer,
		"numeric(5, 0)":     Integer,
		"numeric(5, 2)":     EDecimal,
		"numeric(8, 6)":     EDecimal,
		"bignumeric(38, 2)": EDecimal,

		// Integer
		"int":     Integer,
		"integer": Integer,
		"inT64":   Integer,
		// String
		"varchar":     String,
		"string":      String,
		"sTriNG":      String,
		"STRING (10)": String,
		// Array
		"array<integer>": Array,
		"array<string>":  Array,
		// Boolean
		"bool":    Boolean,
		"boolean": Boolean,
		// Struct
		"STRUCT<foo STRING>": Struct,
		"record":             Struct,
		"json":               Struct,
		// Datetime
		"datetime":  NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType),
		"timestamp": NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType),
		"time":      NewKindDetailsFromTemplate(ETime, ext.TimeKindType),
		"date":      NewKindDetailsFromTemplate(ETime, ext.DateKindType),
		//Invalid
		"foo":    Invalid,
		"foofoo": Invalid,
		"":       Invalid,
	}

	for bqCol, expectedKind := range bqColToExpectedKind {
		kd, err := DwhTypeToKind(constants.BigQuery, bqCol, "")
		if expectedKind.Kind == Invalid.Kind {
			assert.ErrorContains(t, err, fmt.Sprintf("unable to map type: %q to dwh type", bqCol))
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, expectedKind.Kind, kd.Kind, bqCol)
	}
}

func TestBigQueryTypeNoDataLoss(t *testing.T) {
	kindDetails := []KindDetails{
		NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType),
		NewKindDetailsFromTemplate(ETime, ext.TimeKindType),
		NewKindDetailsFromTemplate(ETime, ext.DateKindType),
		String,
		Boolean,
		Struct,
	}

	for _, kindDetail := range kindDetails {
		kd, err := DwhTypeToKind(constants.BigQuery, kindToBigQuery(kindDetail), "")
		assert.NoError(t, err)
		assert.Equal(t, kindDetail, kd)
	}
}

func fromExpiresDateStringToTime(tsString string) (time.Time, error) {
	return time.Parse(bqLayout, tsString)
}

func TestExpiresDate(t *testing.T) {
	// We should be able to go back and forth.
	// Note: The format does not have ns precision because we don't need it.
	birthday := time.Date(2022, time.September, 6, 3, 19, 24, 0, time.UTC)
	for i := 0; i < 5; i++ {
		tsString := ExpiresDate(birthday)
		ts, err := fromExpiresDateStringToTime(tsString)
		assert.NoError(t, err)
		assert.Equal(t, birthday, ts)
	}

	for _, badString := range []string{"foo", "bad_string", " 2022-09-01"} {
		_, err := fromExpiresDateStringToTime(badString)
		assert.ErrorContains(t, err, "cannot parse", badString)
	}
}
