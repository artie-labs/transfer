package typing

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

func RedshiftTypeToKind(rawType string) KindDetails {
	// TODO - this needs to be filled out.
	rawType = strings.ToLower(rawType)

	switch rawType {
	case "integer":
		return Integer
	case "character varying":
		return String
	case "timestamp with time zone", "timestamp without time zone":
		return NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType)
	case "boolean":
		return Boolean
	}

	fmt.Println("RedshiftTypeToKind raw type", rawType)

	return Invalid
}

func kindToRedShift(kd KindDetails) string {
	fmt.Println("kindToRedShift", kd)
	switch kd.Kind {
	case String.Kind:
		return "text"

	case ETime.Kind:
		switch kd.ExtendedTimeDetails.Type {
		case ext.DateTimeKindType:
			return "timestamp with time zone"
		case ext.DateKindType:
			return "date"
		case ext.TimeKindType:
			return "time"
		}
	}

	return kd.Kind
}
