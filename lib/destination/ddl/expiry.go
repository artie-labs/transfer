package ddl

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
)

const (
	ExpireCommentPrefix = "expires:"
	TempTableTTL        = 6 * time.Hour
)

func ExpiryComment(expiryString string) string {
	return fmt.Sprintf("%s%s", ExpireCommentPrefix, expiryString)
}

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

func ShouldDeleteUnix(unixString string) bool {
	unix, err := strconv.Atoi(unixString)
	if err != nil {
		return false
	}

	ts := time.Unix(int64(unix), 0)
	return time.Now().UTC().After(ts)
}
