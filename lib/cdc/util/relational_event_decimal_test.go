package util

import (
	"context"
	"encoding/json"
	"io"
	"math/big"
	"os"
	"testing"

	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/config"

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

	retMap := schemaEventPayload.GetData(context.Background(), nil, nil)
	assert.Equal(t, retMap["smallint_test"], 1)
	assert.Equal(t, retMap["smallserial_test"], 2)
	assert.Equal(t, retMap["int_test"], 3)
	assert.Equal(t, retMap["integer_test"], 4)
	assert.Equal(t, retMap["serial_test"], 1)
	assert.Equal(t, retMap["bigint_test"], 2305843009213693952)
	assert.Equal(t, retMap["bigserial_test"], 2305843009213693952)
}

func TestSchemaEventPayload_Numeric_GetData(t *testing.T) {
	file, err := os.Open("./numeric.json")
	assert.NoError(t, err)
	bytes, err := io.ReadAll(file)
	assert.NoError(t, err)

	var schemaEventPayload SchemaEventPayload
	err = json.Unmarshal(bytes, &schemaEventPayload)
	assert.NoError(t, err)
	retMap := schemaEventPayload.GetData(context.Background(), nil, nil)

	assert.Equal(t, "123456.789", retMap["numeric_test"].(*decimal.Decimal).Value())
	assert.Equal(t, 0, big.NewFloat(1234).Cmp(retMap["numeric_5"].(*decimal.Decimal).Value().(*big.Float)))
	numericWithScaleMap := map[string]string{
		"numeric_5_2": "568.01",
		"numeric_5_6": "0.023456",
		"numeric_5_0": "5",
	}

	for key, expectedValue := range numericWithScaleMap {
		// Numeric data types that actually have scale fails when comparing *big.Float using `.Cmp`, so we are using STRING() instead.
		_, isOk := retMap[key].(*decimal.Decimal).Value().(*big.Float)
		assert.True(t, isOk)
		// Now, we know the data type is *big.Float, let's check the .String() value.
		assert.Equal(t, expectedValue, retMap[key].(*decimal.Decimal).String())
	}

	assert.Equal(t, "58569102859845154622791691858438258688", retMap["numeric_39_0"].(*decimal.Decimal).Value())
	assert.Equal(t, "5856910285984515462279169185843825868.22", retMap["numeric_39_2"].(*decimal.Decimal).Value())
	assert.Equal(t, "585691028598451546227958438258688.123456", retMap["numeric_39_6"].(*decimal.Decimal).Value())
}

func TestSchemaEventPayload_Decimal_GetData(t *testing.T) {
	file, err := os.Open("./decimal.json")
	assert.NoError(t, err)
	bytes, err := io.ReadAll(file)
	assert.NoError(t, err)

	ctx := context.Background()
	ctx = config.InjectSettingsIntoContext(ctx, &config.Settings{Config: nil, VerboseLogging: true})
	var schemaEventPayload SchemaEventPayload
	err = json.Unmarshal(bytes, &schemaEventPayload)
	assert.NoError(t, err)
	retMap := schemaEventPayload.GetData(ctx, nil, nil)
	assert.Equal(t, "123.45", retMap["decimal_test"].(*decimal.Decimal).Value())
	decimalWithScaleMap := map[string]string{
		"decimal_test_5":   "123",
		"decimal_test_5_2": "123.45",
	}

	for key, expectedValue := range decimalWithScaleMap {
		// Numeric data types that actually have scale fails when comparing *big.Float using `.Cmp`, so we are using STRING() instead.
		_, isOk := retMap[key].(*decimal.Decimal).Value().(*big.Float)
		assert.True(t, isOk)
		// Now, we know the data type is *big.Float, let's check the .String() value.
		assert.Equal(t, expectedValue, retMap[key].(*decimal.Decimal).String(), key)
	}

	assert.Equal(t, "58569102859845154622791691858438258688", retMap["decimal_test_39"].(*decimal.Decimal).Value(), "decimal_test_39")
	assert.Equal(t, "585691028598451546227916918584382586.22", retMap["decimal_test_39_2"].(*decimal.Decimal).Value(), "decimal_test_39_2")
	assert.Equal(t, "585691028598451546227916918584388.123456", retMap["decimal_test_39_6"].(*decimal.Decimal).Value(), "decimal_test_39_6")
}

func TestSchemaEventPayload_Money_GetData(t *testing.T) {
	file, err := os.Open("./money.json")
	assert.NoError(t, err)
	bytes, err := io.ReadAll(file)
	assert.NoError(t, err)

	ctx := context.Background()
	ctx = config.InjectSettingsIntoContext(ctx, &config.Settings{Config: nil, VerboseLogging: true})
	var schemaEventPayload SchemaEventPayload
	err = json.Unmarshal(bytes, &schemaEventPayload)
	assert.NoError(t, err)
	retMap := schemaEventPayload.GetData(ctx, nil, nil)

	decimalWithScaleMap := map[string]string{
		"money_test": "123456.78",
	}

	for key, expectedValue := range decimalWithScaleMap {
		// Numeric data types that actually have scale fails when comparing *big.Float using `.Cmp`, so we are using STRING() instead.
		_, isOk := retMap[key].(*decimal.Decimal).Value().(*big.Float)
		assert.True(t, isOk)
		// Now, we know the data type is *big.Float, let's check the .String() value.
		assert.Equal(t, expectedValue, retMap[key].(*decimal.Decimal).String(), key)
	}
}
