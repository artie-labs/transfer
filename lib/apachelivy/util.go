package apachelivy

import (
	"github.com/artie-labs/transfer/clients/iceberg/dialect"
)

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	_dialect := dialect.IcebergDialect{}
	if _dialect.IsTableDoesNotExistErr(err) {
		return false
	} else if _dialect.IsColumnAlreadyExistsErr(err) {
		return false
	}

	return true
}
