package bigquery

import "strings"

func isTableQuotaError(err error) bool {
	return strings.Contains(err.Error(), "Exceeded rate limits: too many table update operations for this table")
}

func (s *Store) IsRetryableError(err error) bool {
	if isTableQuotaError(err) {
		return true
	}

	return s.Store.IsRetryableError(err)
}
