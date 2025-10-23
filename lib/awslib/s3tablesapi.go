package awslib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/aws/smithy-go"
)

// Full API spec can be seen here: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_S3_Tables.html
type S3TablesAPIWrapper struct {
	client         *s3tables.Client
	s3Client       *s3.Client
	tableBucketARN string
}

func NewS3TablesAPI(cfg aws.Config, tableBucketARN string) S3TablesAPIWrapper {
	return S3TablesAPIWrapper{
		client:         s3tables.NewFromConfig(cfg),
		s3Client:       s3.NewFromConfig(cfg),
		tableBucketARN: tableBucketARN,
	}
}

func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFoundException" {
		return true
	}

	return false
}

func IsConflictError(err error) bool {
	if err == nil {
		return false
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "ConflictException" {
		return true
	}

	return false
}

func (s S3TablesAPIWrapper) GetTableBucket(ctx context.Context) (s3tables.GetTableBucketOutput, error) {
	resp, err := s.client.GetTableBucket(ctx, &s3tables.GetTableBucketInput{
		TableBucketARN: aws.String(s.tableBucketARN),
	})
	if err != nil {
		return s3tables.GetTableBucketOutput{}, err
	}

	return *resp, nil
}

func (s S3TablesAPIWrapper) GetNamespace(ctx context.Context, namespace string) (s3tables.GetNamespaceOutput, error) {
	resp, err := s.client.GetNamespace(ctx, &s3tables.GetNamespaceInput{
		Namespace:      aws.String(namespace),
		TableBucketARN: aws.String(s.tableBucketARN),
	})
	if err != nil {
		return s3tables.GetNamespaceOutput{}, err
	}

	return *resp, nil
}

func (s S3TablesAPIWrapper) ListNamespaces(ctx context.Context) ([]types.NamespaceSummary, error) {
	var res []types.NamespaceSummary
	var continuationToken *string

	for {
		resp, err := s.client.ListNamespaces(ctx, &s3tables.ListNamespacesInput{
			TableBucketARN:    aws.String(s.tableBucketARN),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return []types.NamespaceSummary{}, err
		}

		res = append(res, resp.Namespaces...)
		if resp.ContinuationToken != nil {
			continuationToken = resp.ContinuationToken
		} else {
			break
		}
	}

	return res, nil
}

func (s S3TablesAPIWrapper) CreateNamespace(ctx context.Context, namespace string) error {
	_, err := s.client.CreateNamespace(ctx, &s3tables.CreateNamespaceInput{
		// Namespace is a fixed list with one element
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3TableBuckets_CreateNamespace.html#AmazonS3-s3TableBuckets_CreateNamespace-request-namespace
		Namespace:      []string{namespace},
		TableBucketARN: aws.String(s.tableBucketARN),
	})

	// Swallow the conflict error to avoid false positive error since it may have been created by another request.
	if IsConflictError(err) {
		return nil
	}

	return err
}

// ListTables requires the namespace to be exact match and is case sensitive
func (s S3TablesAPIWrapper) ListTables(ctx context.Context, namespace string) ([]types.TableSummary, error) {
	var tables []types.TableSummary
	var continuationToken *string

	for {
		resp, err := s.client.ListTables(ctx, &s3tables.ListTablesInput{
			Namespace:         aws.String(namespace),
			TableBucketARN:    aws.String(s.tableBucketARN),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return []types.TableSummary{}, err
		}

		tables = append(tables, resp.Tables...)
		if resp.ContinuationToken != nil {
			continuationToken = resp.ContinuationToken
		} else {
			break
		}
	}

	return tables, nil
}

func (s S3TablesAPIWrapper) GetTable(ctx context.Context, namespace, table string) (s3tables.GetTableOutput, error) {
	resp, err := s.client.GetTable(ctx, &s3tables.GetTableInput{
		Namespace:      aws.String(namespace),
		Name:           aws.String(table),
		TableBucketARN: aws.String(s.tableBucketARN),
	})
	if err != nil {
		return s3tables.GetTableOutput{}, err
	}

	return *resp, nil
}

func (s S3TablesAPIWrapper) GetTableMetadata(ctx context.Context, s3URI string) (S3TableSchema, error) {
	body, err := s.readFromS3URI(ctx, s3URI)
	if err != nil {
		return S3TableSchema{}, err
	}

	var tableSchema S3TableSchema
	if err = json.Unmarshal([]byte(body), &tableSchema); err != nil {
		return S3TableSchema{}, err
	}

	return tableSchema, nil
}

func (s S3TablesAPIWrapper) DeleteTable(ctx context.Context, namespace, table string) error {
	_, err := s.client.DeleteTable(ctx, &s3tables.DeleteTableInput{
		Namespace:      aws.String(namespace),
		Name:           aws.String(table),
		TableBucketARN: aws.String(s.tableBucketARN),
	})

	// Swallow the not found error to avoid false positive error and align with other databases.
	if IsNotFoundError(err) {
		return nil
	}

	return err
}

func (s S3TablesAPIWrapper) readFromS3URI(ctx context.Context, s3URI string) (string, error) {
	bucket, key, found := strings.Cut(strings.TrimPrefix(s3URI, "s3://"), "/")
	if !found {
		return "", fmt.Errorf("invalid s3URI: %q", s3URI)
	}

	resp, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	return string(body), nil
}
