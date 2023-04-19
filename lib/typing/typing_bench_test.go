package typing

import (
	"reflect"
	"testing"
)

func BenchmarkParseValueIntegerArtie(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ParseValue("", nil,45456312)
	}
}

func BenchmarkParseValueIntegerGo(b *testing.B) {
	for n := 0; n < b.N; n++ {
		reflect.TypeOf(45456312).Kind()
	}
}

func BenchmarkParseValueBooleanArtie(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ParseValue("", nil, true)
	}
}

func BenchmarkParseValueBooleanGo(b *testing.B) {
	for n := 0; n < b.N; n++ {
		reflect.TypeOf(true).Kind()
	}
}

func BenchmarkParseValueFloatArtie(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ParseValue("", nil, 7.44)
	}
}

func BenchmarkParseValueFloatGo(b *testing.B) {
	for n := 0; n < b.N; n++ {
		reflect.TypeOf(7.44).Kind()
	}
}
