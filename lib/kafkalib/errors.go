package kafkalib

import "strings"

func IsRetryableErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Group Authorization Failed")
}
