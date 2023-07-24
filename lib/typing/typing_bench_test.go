package typing

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func BenchmarkLargeMapLengthQuery(b *testing.B) {
	retMap := make(map[string]interface{})
	for i := 0; i < 15000; i++ {
		retMap[fmt.Sprintf("key-%v", i)] = true
	}

	for n := 0; n < b.N; n++ {
		_ = uint(len(retMap))
	}
}

func BenchmarkLargeMapLengthQuery_WithMassiveValues(b *testing.B) {
	retMap := make(map[string]interface{})
	for i := 0; i < 15000; i++ {
		retMap[fmt.Sprintf("key-%v", i)] = map[string]interface{}{
			"foo":   "bar",
			"hello": "world",
			"true":  true,
			"false": false,
			"array": []string{"abc", "def"},
		}
	}

	for n := 0; n < b.N; n++ {
		_ = uint(len(retMap))
	}
}

func BenchmarkParseValueIntegerArtie(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ParseValue(context.Background(), "", nil, 45456312)
	}
}

func BenchmarkParseValueIntegerGo(b *testing.B) {
	for n := 0; n < b.N; n++ {
		reflect.TypeOf(45456312).Kind()
	}
}

func BenchmarkParseValueBooleanArtie(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ParseValue(context.Background(), "", nil, true)
	}
}

func BenchmarkParseValueBooleanGo(b *testing.B) {
	for n := 0; n < b.N; n++ {
		reflect.TypeOf(true).Kind()
	}
}

func BenchmarkParseValueFloatArtie(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ParseValue(context.Background(), "", nil, 7.44)
	}
}

func BenchmarkParseValueFloatGo(b *testing.B) {
	for n := 0; n < b.N; n++ {
		reflect.TypeOf(7.44).Kind()
	}
}
