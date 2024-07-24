package converters

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

func ConvertDateTimeWithTimezone(value any) (*ext.ExtendedTime, error) {
	dtString, isOk := value.(string)
	if !isOk {
		return nil, fmt.Errorf("expected string got '%v' with type %T", value, value)
	}

	// We don't need to pass `additionalDateFormats` because this data type layout is standardized by Debezium
	extTime, err := ext.ParseExtendedDateTime(dtString, nil)
	if err == nil {
		return extTime, nil
	}

	// Check for negative years
	if strings.HasPrefix(dtString, "-") {
		return nil, nil
	}

	if parts := strings.Split(dtString, "-"); len(parts) == 3 {
		// Check if year exceeds 9999
		if len(parts[0]) > 4 {
			return nil, nil
		}
	}

	return nil, fmt.Errorf("failed to parse %q, err: %w", dtString, err)
}
