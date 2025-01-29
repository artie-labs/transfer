package ddl

import (
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
)

func ShouldDeleteFromName(name string) bool {
	parts := strings.Split(strings.ToLower(name), constants.ArtiePrefix)
	if len(parts) != 2 {
		return false
	}

	suffixParts := strings.Split(parts[1], "_")
	if len(suffixParts) != 3 {
		return false
	}

	part := suffixParts[2]
	if part == "msm" {
		return false
	}

	unix, err := strconv.Atoi(part)
	if err != nil {
		slog.Error("Failed to parse unix string",
			slog.Any("err", err),
			slog.String("tableName", name),
			slog.String("tsString", suffixParts[2]),
		)
		return false
	}

	return time.Now().UTC().After(time.Unix(int64(unix), 0))
}
