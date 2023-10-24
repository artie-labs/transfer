package bigquery

import "strings"

func TableUpdateQuotaErr(err error) bool {
	return strings.Contains(err.Error(), "Exceeded rate limits: too many table update operations for this table")
}
