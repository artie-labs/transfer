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
	switch castedValue := value.(type) {
	case int64:
		date := time.UnixMilli(0).In(time.UTC).AddDate(0, 0, int(castedValue))
		if date.Year() > 9999 {
			slog.Warn("Date exceeds 9999 year, setting this to null to avoid encoding errors", slog.Int("year", date.Year()))
			return nil, nil
		}

		// Represents the number of days since the epoch.
		return date.Format(time.DateOnly), nil
	case string:
		if _, err := time.Parse(time.DateOnly, castedValue); err != nil {
			return nil, fmt.Errorf("failed to parse date: %w", err)
		}

		return castedValue, nil
	default:
		return nil, fmt.Errorf("expected int64 or string got '%v' with type %T", value, value)
	}
}
