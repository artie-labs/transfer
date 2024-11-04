package converters

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Date struct{}

func (Date) layout() string {
	return ext.PostgresDateFormat
}

func (d Date) ToKindDetails() typing.KindDetails {
	return typing.Date
}

func (d Date) Convert(value any) (any, error) {
	valueInt64, isOk := value.(int64)
	if !isOk {
		return nil, fmt.Errorf("expected int64 got '%v' with type %T", value, value)
	}

	// Represents the number of days since the epoch.
	return time.UnixMilli(0).In(time.UTC).AddDate(0, 0, int(valueInt64)), nil
}
