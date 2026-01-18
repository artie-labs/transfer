package sqs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/google/uuid"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	desttypes "github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	sqllib "github.com/artie-labs/transfer/lib/sql"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
)

const (
	// SQS allows up to 10 messages per batch
	maxBatchSize = 10
)

type Store struct {
	config    config.Config
	sqsClient *sqs.Client
	configMap *desttypes.DestinationTableConfigMap
}

func (s *Store) GetConfig() config.Config {
	return s.config
}

func (s *Store) Validate() error {
	return s.config.SQS.Validate()
}

func (s *Store) IdentifierFor(topicConfig kafkalib.DatabaseAndSchemaPair, table string) sqllib.TableIdentifier {
	return NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table)
}

func (s *Store) Dialect() sqllib.Dialect {
	// SQS doesn't use SQL dialects
	return nil
}

func (s *Store) Dedupe(_ context.Context, _ sqllib.TableIdentifier, _ []string, _ bool) error {
	return fmt.Errorf("dedupe is not supported for SQS")
}

func (s *Store) GetTableConfig(_ context.Context, tableID sqllib.TableIdentifier, _ bool) (*desttypes.DestinationTableConfig, error) {
	tableConfig := s.configMap.GetTableConfig(tableID)
	if tableConfig == nil {
		tableConfig = desttypes.NewDestinationTableConfig(nil, false)
		s.configMap.AddTable(tableID, tableConfig)
	}
	return tableConfig, nil
}

func (s *Store) SweepTemporaryTables(_ context.Context, _ *webhooksclient.Client) error {
	return nil
}

func (s *Store) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, fmt.Errorf("ExecContext is not supported for SQS")
}

func (s *Store) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, fmt.Errorf("QueryContext is not supported for SQS")
}

func (s *Store) Begin() (*sql.Tx, error) {
	return nil, fmt.Errorf("transactions are not supported for SQS")
}

func (s *Store) LoadDataIntoTable(_ context.Context, _ *optimization.TableData, _ *desttypes.DestinationTableConfig, _, _ sqllib.TableIdentifier, _ desttypes.AdditionalSettings, _ bool) error {
	return fmt.Errorf("LoadDataIntoTable is not supported for SQS")
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, _ bool) error {
	// For SQS, append and merge are identical - we always send new messages
	if _, err := s.Merge(ctx, tableData, whClient); err != nil {
		return fmt.Errorf("failed to append: %w", err)
	}
	return nil
}

// getQueueURL returns the appropriate queue URL based on configuration mode
func (s *Store) getQueueURL(ctx context.Context, tableID TableIdentifier) (string, error) {
	sqsSettings := s.config.SQS

	if sqsSettings.IsSingleQueueMode() {
		return sqsSettings.QueueURL, nil
	}

	// Per-table mode: construct queue URL from queue name
	queueName := tableID.QueueName()
	if sqsSettings.UseFIFO && !strings.HasSuffix(queueName, ".fifo") {
		queueName = queueName + ".fifo"
	}

	result, err := s.sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get queue URL for %q: %w", queueName, err)
	}

	return *result.QueueUrl, nil
}

// Merge sends all rows from TableData as JSON messages to SQS
func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData, _ *webhooksclient.Client) (bool, error) {
	if tableData.ShouldSkipUpdate() {
		return false, nil
	}

	tableID := s.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	sqsTableID, ok := tableID.(TableIdentifier)
	if !ok {
		return false, fmt.Errorf("expected tableID to be a TableIdentifier, got %T", tableID)
	}

	rows := tableData.Rows()
	if len(rows) == 0 {
		return false, nil
	}

	queueURL, err := s.getQueueURL(ctx, sqsTableID)
	if err != nil {
		return false, err
	}

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	sqsSettings := s.config.SQS

	var totalSent int

	// Process rows in batches of 10 (SQS limit)
	for batch := range slices.Chunk(rows, maxBatchSize) {
		entries := make([]types.SendMessageBatchRequestEntry, 0, len(batch))

		for i, row := range batch {
			rowData := make(map[string]any)
			for _, col := range cols {
				value, _ := row.GetValue(col.Name())
				rowData[col.Name()] = value
			}

			jsonData, err := json.Marshal(rowData)
			if err != nil {
				return false, fmt.Errorf("failed to marshal row data: %w", err)
			}

			entry := types.SendMessageBatchRequestEntry{
				Id:          aws.String(fmt.Sprintf("msg-%d", i)),
				MessageBody: aws.String(string(jsonData)),
			}

			// FIFO queue settings
			if sqsSettings.UseFIFO {
				entry.MessageGroupId = aws.String(sqsSettings.MessageGroupID)
				if sqsSettings.UseDeduplicationID {
					// Use UUID for deduplication to ensure uniqueness
					entry.MessageDeduplicationId = aws.String(uuid.New().String())
				}
			}

			entries = append(entries, entry)
		}

		output, err := s.sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueURL),
			Entries:  entries,
		})
		if err != nil {
			return false, fmt.Errorf("failed to send message batch to SQS: %w", err)
		}

		// Check for partial failures
		if len(output.Failed) > 0 {
			var failedIDs []string
			for _, failed := range output.Failed {
				failedIDs = append(failedIDs, *failed.Id)
				slog.Error("Failed to send SQS message",
					slog.String("id", *failed.Id),
					slog.String("code", *failed.Code),
					slog.String("message", *failed.Message),
				)
			}
			return false, fmt.Errorf("failed to send %d messages: %v", len(output.Failed), failedIDs)
		}

		totalSent += len(output.Successful)
	}

	slog.Info("Successfully sent messages to SQS",
		slog.String("queueURL", queueURL),
		slog.Int("messageCount", totalSent),
	)

	return true, nil
}

func (s *Store) IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for standard network errors
	if db.IsRetryableError(err) {
		return true
	}

	errMsg := err.Error()

	// SQS-specific retryable errors
	retryableStrings := []string{
		"ThrottlingException",
		"ServiceUnavailable",
		"InternalError",
		"RequestThrottled",
		"OverLimit",
	}

	for _, retryable := range retryableStrings {
		if strings.Contains(errMsg, retryable) {
			return true
		}
	}

	return false
}

func (s *Store) DropTable(ctx context.Context, tableID sqllib.TableIdentifier) error {
	// For SQS, "dropping a table" means purging the queue (if in per-table mode)
	// In single queue mode, we can't purge because it affects all tables
	sqsSettings := s.config.SQS

	if sqsSettings.IsSingleQueueMode() {
		slog.Warn("Cannot purge queue in single queue mode - would affect all tables")
		s.configMap.RemoveTable(tableID)
		return nil
	}

	sqsTableID, ok := tableID.(TableIdentifier)
	if !ok {
		return fmt.Errorf("expected tableID to be a TableIdentifier, got %T", tableID)
	}

	queueURL, err := s.getQueueURL(ctx, sqsTableID)
	if err != nil {
		// Queue might not exist, which is fine for drop
		slog.Warn("Could not get queue URL for drop operation", slog.Any("err", err))
		s.configMap.RemoveTable(tableID)
		return nil
	}

	_, err = s.sqsClient.PurgeQueue(ctx, &sqs.PurgeQueueInput{
		QueueUrl: aws.String(queueURL),
	})
	if err != nil {
		slog.Warn("Failed to purge SQS queue", slog.Any("err", err), slog.String("queueURL", queueURL))
	} else {
		slog.Info("Purged SQS queue", slog.String("queueURL", queueURL))
	}

	s.configMap.RemoveTable(tableID)
	return nil
}

func LoadSQS(ctx context.Context, cfg config.Config) (destination.Destination, error) {
	if cfg.SQS == nil {
		return nil, fmt.Errorf("sqs config is nil")
	}

	sqsSettings := cfg.SQS

	// Build AWS config
	var awsCfg aws.Config
	var err error

	if sqsSettings.AwsAccessKeyID != "" && sqsSettings.AwsSecretAccessKey != "" {
		// Use static credentials
		creds := credentials.NewStaticCredentialsProvider(
			sqsSettings.AwsAccessKeyID,
			sqsSettings.AwsSecretAccessKey,
			"",
		)
		awsCfg, err = awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithCredentialsProvider(creds),
			awsconfig.WithRegion(sqsSettings.AwsRegion),
		)
	} else {
		// Use default credential chain (IAM role, environment, etc.)
		awsCfg, err = awsconfig.LoadDefaultConfig(ctx,
			awsconfig.WithRegion(sqsSettings.AwsRegion),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// If role ARN is specified, assume the role
	if sqsSettings.RoleARN != "" {
		stsClient := sts.NewFromConfig(awsCfg)
		creds := stscreds.NewAssumeRoleProvider(stsClient, sqsSettings.RoleARN)
		awsCfg.Credentials = aws.NewCredentialsCache(creds)
	}

	sqsClient := sqs.NewFromConfig(awsCfg)

	store := &Store{
		config:    cfg,
		sqsClient: sqsClient,
		configMap: &desttypes.DestinationTableConfigMap{},
	}

	if err := store.Validate(); err != nil {
		return nil, err
	}

	slog.Info("Successfully initialized SQS client",
		slog.String("region", sqsSettings.AwsRegion),
		slog.Bool("singleQueueMode", sqsSettings.IsSingleQueueMode()),
		slog.Bool("fifo", sqsSettings.UseFIFO),
	)

	return store, nil
}
