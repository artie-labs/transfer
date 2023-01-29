package bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"github.com/artie-labs/transfer/lib/logger"
)

type Client interface {
	Query(q string) *bigquery.Query
}

func NewClient(ctx context.Context, projectID string) Client {
	// We can explore removing this and standardize on sql/database once this library addresses this bug: https://github.com/viant/bigquery/issues/5
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		logger.FromContext(ctx).WithError(err).Fatalf("failed to start bigquery client, err: %v", err)
	}

	return client
}
