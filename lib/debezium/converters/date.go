package converters

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
)

type Date struct{}

func (d Date) ToKindDetails() typing.KindDetails {
	return typing.Date
}

func (d Date) Convert(value any) (any, error) {
	valueInt64, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("expected int64 got '%v' with type %T", value, value)
	}

	date := time.UnixMilli(0).In(time.UTC).AddDate(0, 0, int(valueInt64))
	if date.Year() > 9999 {
		slog.Warn("Date exceeds 9999 year, setting this to null to avoid encoding errors", slog.Int("year", date.Year()))
		return nil, nil
	}

	// Represents the number of days since the epoch.
	return date.Format(time.DateOnly), nil
}
