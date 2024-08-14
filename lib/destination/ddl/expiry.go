package ddl

import (
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
)

func ShouldDeleteFromName(name string) bool {
	if !strings.Contains(strings.ToLower(name), constants.ArtiePrefix) {
		return false
	}

	nameParts := strings.Split(name, "_")
	if len(nameParts) < 2 {
		return false
	}

	unixString := nameParts[len(nameParts)-1]
	unix, err := strconv.Atoi(unixString)
	if err != nil {
		slog.Error("Failed to parse unix string", slog.Any("err", err), slog.String("tableName", name), slog.String("unixString", unixString))
		return false
	}

	ts := time.Unix(int64(unix), 0)
	return time.Now().UTC().After(ts)
}
