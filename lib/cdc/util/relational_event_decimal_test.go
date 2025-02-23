package util

import (
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/stretchr/testify/assert"
)

func TestSchemaEventPayload_MiscNumbers_GetData(t *testing.T) {
	file, err := os.Open("./numbers.json")
	assert.NoError(t, err)
	bytes, err := io.ReadAll(file)
	assert.NoError(t, err)

	var schemaEventPayload SchemaEventPayload
	err = json.Unmarshal(bytes, &schemaEventPayload)
	assert.NoError(t, err)

	retMap, err := schemaEventPayload.GetData(kafkalib.TopicConfig{})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), retMap["smallint_test"])
	assert.Equal(t, int64(2), retMap["smallserial_test"])
	assert.Equal(t, int64(3), retMap["int_test"])
	assert.Equal(t, int64(4), retMap["integer_test"])
	assert.Equal(t, int64(1), retMap["serial_test"])
	assert.Equal(t, int64(2305843009213693952), retMap["bigint_test"])
	assert.Equal(t, int64(2305843009213693952), retMap["bigserial_test"])
}

func TestSchemaEventPayload_Numeric_GetData(t *testing.T) {
	file, err := os.Open("./numeric.json")
	assert.NoError(t, err)
	bytes, err := io.ReadAll(file)
	assert.NoError(t, err)

	var schemaEventPayload SchemaEventPayload
	err = json.Unmarshal(bytes, &schemaEventPayload)
	assert.NoError(t, err)
	retMap, err := schemaEventPayload.GetData(kafkalib.TopicConfig{})
	assert.NoError(t, err)

	assert.Equal(t, "123456.789", retMap["numeric_test"].(*decimal.Decimal).String())
	assert.Equal(t, "1234", retMap["numeric_5"].(*decimal.Decimal).String())
	numericWithScaleMap := map[string]string{
		"numeric_5_2": "568.01",
		"numeric_5_6": "0.023456",
		"numeric_5_0": "5",
	}

	for key, expectedValue := range numericWithScaleMap {
		assert.Equal(t, expectedValue, retMap[key].(*decimal.Decimal).String())
	}

	assert.Equal(t, "58569102859845154622791691858438258688", retMap["numeric_39_0"].(*decimal.Decimal).String())
	assert.Equal(t, "5856910285984515462279169185843825868.22", retMap["numeric_39_2"].(*decimal.Decimal).String())
	assert.Equal(t, "585691028598451546227958438258688.123456", retMap["numeric_39_6"].(*decimal.Decimal).String())
}

func TestSchemaEventPayload_Decimal_GetData(t *testing.T) {
	file, err := os.Open("./decimal.json")
	assert.NoError(t, err)
	bytes, err := io.ReadAll(file)
	assert.NoError(t, err)

	var schemaEventPayload SchemaEventPayload
	err = json.Unmarshal(bytes, &schemaEventPayload)
	assert.NoError(t, err)
	retMap, err := schemaEventPayload.GetData(kafkalib.TopicConfig{})
	assert.NoError(t, err)
	assert.Equal(t, "123.45", retMap["decimal_test"].(*decimal.Decimal).String())
	decimalWithScaleMap := map[string]string{
		"decimal_test_5":   "123",
		"decimal_test_5_2": "123.45",
	}

	for key, expectedValue := range decimalWithScaleMap {
		assert.Equal(t, expectedValue, retMap[key].(*decimal.Decimal).String(), key)
	}

	assert.Equal(t, "58569102859845154622791691858438258688", retMap["decimal_test_39"].(*decimal.Decimal).String(), "decimal_test_39")
	assert.Equal(t, "585691028598451546227916918584382586.22", retMap["decimal_test_39_2"].(*decimal.Decimal).String(), "decimal_test_39_2")
	assert.Equal(t, "585691028598451546227916918584388.123456", retMap["decimal_test_39_6"].(*decimal.Decimal).String(), "decimal_test_39_6")
}

func TestSchemaEventPayload_Money_GetData(t *testing.T) {
	file, err := os.Open("./money.json")
	assert.NoError(t, err)
	bytes, err := io.ReadAll(file)
	assert.NoError(t, err)

	var schemaEventPayload SchemaEventPayload
	err = json.Unmarshal(bytes, &schemaEventPayload)
	assert.NoError(t, err)
	retMap, err := schemaEventPayload.GetData(kafkalib.TopicConfig{})
	assert.NoError(t, err)

	decimalWithScaleMap := map[string]string{
		"money_test": "123456.78",
	}

	for key, expectedValue := range decimalWithScaleMap {
		assert.Equal(t, expectedValue, retMap[key].(*decimal.Decimal).String(), key)
	}
}
