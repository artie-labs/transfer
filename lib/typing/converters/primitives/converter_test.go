package primitives

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInt64Converter_Convert(t *testing.T) {
	converter := Int64Converter{}
	{
		// Test converting valid string to int64
		got, err := converter.Convert("123")
		assert.NoError(t, err)
		assert.Equal(t, int64(123), got)
	}
	{
		// Test error handling for invalid string
		got, err := converter.Convert("not a number")
		assert.Error(t, err)
		assert.Equal(t, int64(0), got)
	}
	{
		// Test converting int16 to int64
		got, err := converter.Convert(int16(123))
		assert.NoError(t, err)
		assert.Equal(t, int64(123), got)
	}
	{
		// Test converting int32 to int64
		got, err := converter.Convert(int32(456))
		assert.NoError(t, err)
		assert.Equal(t, int64(456), got)
	}
	{
		// Test converting int to int64
		got, err := converter.Convert(789)
		assert.NoError(t, err)
		assert.Equal(t, int64(789), got)
	}
	{
		// Test converting int64 to int64
		got, err := converter.Convert(int64(101112))
		assert.NoError(t, err)
		assert.Equal(t, int64(101112), got)
	}
	{
		// Test error handling for unsupported type
		got, err := converter.Convert(float64(123.45))
		assert.Error(t, err)
		assert.Equal(t, int64(0), got)
	}
}

func TestBooleanConverter_Convert(t *testing.T) {
	converter := BooleanConverter{}

	trueVariants := []any{"true", true}
	for _, variant := range trueVariants {
		got, err := converter.Convert(variant)
		assert.NoError(t, err)
		assert.True(t, got)
	}

	falseVariants := []any{"false", false}
	for _, variant := range falseVariants {
		got, err := converter.Convert(variant)
		assert.NoError(t, err)
		assert.False(t, got)
	}
}
