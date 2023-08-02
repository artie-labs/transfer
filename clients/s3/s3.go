package s3

import (
	"context"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/optimization"
)

type S3 struct {
	//..
}

func (s *S3) Label() constants.DestinationKind {
	return constants.S3
}

func (s *S3) Merge(ctx context.Context, tableData *optimization.TableData) error {
	return nil
}
