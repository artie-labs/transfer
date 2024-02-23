package typing

import "fmt"

func MsSQLTypeToKind(rawType string, stringPrecision string) KindDetails {
	fmt.Println("rawType", rawType, "stringPrecision", stringPrecision)

	return Invalid
}
