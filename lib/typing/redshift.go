package typing

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

func RedshiftTypeToKind(rawType string) KindDetails {
	rawType = strings.ToLower(rawType)
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
			return "timestamp"
		case ext.DateKindType:
			return "date"
		case ext.TimeKindType:
			return "time"
		}
	}

	return kd.Kind
}
