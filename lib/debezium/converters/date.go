package converters

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Date struct{}

func (Date) ToKindDetails() typing.KindDetails {
	return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType)
}

func (Date) Convert(value any) (any, error) {
	valueInt64, isOk := value.(int64)
	if !isOk {
		return nil, fmt.Errorf("expected int64 got '%v' with type %T", value, value)
	}

	unix := time.UnixMilli(0).In(time.UTC) // 1970-01-01
	// Represents the number of days since the epoch.
	return ext.NewExtendedTime(unix.AddDate(0, 0, int(valueInt64)), ext.DateKindType, ""), nil
}
