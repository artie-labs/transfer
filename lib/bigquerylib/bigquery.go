package bigquerylib

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/bigquery"
)

type Client struct {
	client *bigquery.Client
}

func NewClient(ctx context.Context, client *bigquery.Client) *Client {
	return &Client{client: client}
}

// [UndeleteTable] - Adding this functionality to restore a deleted table.
// Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-undelete-table
func (c Client) UndeleteTable(ctx context.Context, datasetID string, deletedTableName string, restoredTableName string, restoreTime time.Time) error {
	slog.Info("Restoring table",
		slog.String("datasetID", datasetID),
		slog.String("deletedTableName", deletedTableName),
		slog.String("restoredTableName", restoredTableName),
		slog.String("restoreTime", restoreTime.Format(time.RFC3339)),
	)

	ds := c.client.Dataset(datasetID)
	snapshotTableID := fmt.Sprintf("%s@%d", deletedTableName, restoreTime.UnixNano()/1e6)

	// Construct and run a copy job.
	copier := ds.Table(restoredTableName).CopierFrom(ds.Table(snapshotTableID))
	copier.WriteDisposition = bigquery.WriteTruncate
	job, err := copier.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run copy job: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for copy job: %w", err)
	}

	if err := status.Err(); err != nil {
		return fmt.Errorf("failed to wait for copy job: %w", err)
	}

	return nil
}
