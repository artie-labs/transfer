package ddl

import (
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
)

const ExpireCommentPrefix = "expires:"

func ShouldDelete(comment string) (shouldDelete bool) {
	// expires:2023-05-26 05:57:48 UTC
	if strings.HasPrefix(comment, ExpireCommentPrefix) {
		trimmedComment := strings.TrimPrefix(comment, ExpireCommentPrefix)
		ts, err := typing.FromExpiresDateStringToTime(trimmedComment)
		if err != nil {
			return false
		}

		// We should delete it if the time right now is AFTER the ts in the comment.
		return time.Now().After(ts)
	}

	return false
}

func ShouldDeleteFromName(name string) bool {
	nameParts := strings.Split(name, "_")
	if len(nameParts) < 2 {
		return false
	}

	return shouldDeleteUnix(nameParts[len(nameParts)-1])
}

func shouldDeleteUnix(unixString string) bool {
	// TODO: Migrate everyone to use shouldDeleteUnix so we don't need to parse comments.
	unix, err := strconv.Atoi(unixString)
	if err != nil {
		slog.Warn("Failed to parse unix string", slog.Any("err", err), slog.String("unixString", unixString))
		return false
	}

	ts := time.Unix(int64(unix), 0)
	return time.Now().UTC().After(ts)
}
