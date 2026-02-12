package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
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
}

func (s *Store) GetConfig() config.Config {
	return s.config
}

func (s *Store) IsOLTP() bool {
	return false
}

func (s *Store) Validate() error {
	return s.config.SQS.Validate()
}

func (s *Store) IdentifierFor(topicConfig kafkalib.DatabaseAndSchemaPair, table string) sqllib.TableIdentifier {
	return NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table)
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, _ bool) error {
	// For SQS, append and merge are identical - we always send new messages
	if _, err := s.Merge(ctx, tableData, whClient); err != nil {
		return fmt.Errorf("failed to append: %w", err)
	}
	return nil
}

// [buildQueueURL] - returns the appropriate queue URL based on configuration mode
func (s *Store) buildQueueURL(ctx context.Context, tableID TableIdentifier) (string, error) {
	sqsSettings := s.config.SQS
	if sqsSettings.IsSingleQueueMode() {
		return sqsSettings.QueueURL, nil
	}

	// Per-table mode: construct queue URL from queue name
	queueName := tableID.QueueName()
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

	queueURL, err := s.buildQueueURL(ctx, sqsTableID)
	if err != nil {
		return false, err
	}

	var totalSent int
	for batch := range slices.Chunk(tableData.Rows(), maxBatchSize) {
		var entries []types.SendMessageBatchRequestEntry
		for i, row := range batch {
			jsonData, err := json.Marshal(row.GetData())
			if err != nil {
				return false, fmt.Errorf("failed to marshal row data: %w", err)
			}

			entry := types.SendMessageBatchRequestEntry{
				Id:          aws.String(fmt.Sprintf("msg-%d", i)),
				MessageBody: aws.String(string(jsonData)),
			}

			entries = append(entries, entry)
		}

		input := &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueURL),
			Entries:  entries,
		}

		output, err := s.sqsClient.SendMessageBatch(ctx, input)
		if err != nil {
			return false, fmt.Errorf("failed to send message batch to SQS: %w", err)
		}

		// Check for partial failures
		if len(output.Failed) > 0 {
			return false, fmt.Errorf("failed to send %d messages: %q", len(output.Failed), firstError(output.Failed))
		}

		totalSent += len(output.Successful)
	}

	if len(tableData.Rows()) != totalSent {
		return false, fmt.Errorf("expected %d messages to be sent, got %d", len(tableData.Rows()), totalSent)
	}

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

	// Check for SQS-specific typed errors
	var requestThrottled *types.RequestThrottled
	if errors.As(err, &requestThrottled) {
		return true
	}

	var overLimit *types.OverLimit
	if errors.As(err, &overLimit) {
		return true
	}

	// Check for generic smithy API errors (covers service unavailable, internal errors, etc.)
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "ServiceUnavailable", "InternalError", "InternalServiceError":
			return true
		}
		// Server-side faults are generally retryable
		if apiErr.ErrorFault() == smithy.FaultServer {
			return true
		}
	}

	return false
}

func (s *Store) DropTable(ctx context.Context, tableID sqllib.TableIdentifier) error {
	// For SQS, "dropping a table" means purging the queue (if in per-table mode)
	// In single queue mode, we can't purge because it affects all tables
	if s.config.SQS.IsSingleQueueMode() {
		slog.Warn("Cannot purge queue in single queue mode - would affect all tables")
		return nil
	}

	sqsTableID, ok := tableID.(TableIdentifier)
	if !ok {
		return fmt.Errorf("expected tableID to be a TableIdentifier, got %T", tableID)
	}

	queueURL, err := s.buildQueueURL(ctx, sqsTableID)
	if err != nil {
		return fmt.Errorf("failed to get queue URL: %w", err)
	}

	if _, err = s.sqsClient.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: aws.String(queueURL)}); err != nil {
		return fmt.Errorf("failed to purge SQS queue: %w", err)
	}

	return nil
}

func LoadSQS(ctx context.Context, cfg config.Config) (*Store, error) {
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

	store := &Store{
		config:    cfg,
		sqsClient: sqs.NewFromConfig(awsCfg),
	}

	if err := store.Validate(); err != nil {
		return nil, err
	}

	return store, nil
}
