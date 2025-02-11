package awslib

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
)

// Full API spec can be seen here: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_S3_Tables.html
type S3TablesAPIWrapper struct {
	client         *s3tables.Client
	tableBucketARN string
}

func NewS3TablesAPI(cfg aws.Config, tableBucketARN string) S3TablesAPIWrapper {
	return S3TablesAPIWrapper{
		client:         s3tables.NewFromConfig(cfg),
		tableBucketARN: tableBucketARN,
	}
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

	return err
}

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

func (s S3TablesAPIWrapper) GetTable(ctx context.Context, namespace string, table string) (s3tables.GetTableOutput, error) {
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

func (s S3TablesAPIWrapper) DeleteTable(ctx context.Context, namespace string, table string) error {
	_, err := s.client.DeleteTable(ctx, &s3tables.DeleteTableInput{
		Namespace:      aws.String(namespace),
		Name:           aws.String(table),
		TableBucketARN: aws.String(s.tableBucketARN),
	})

	return err
}
