package awslib

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
)

type S3TablesAPI struct {
	client         *s3tables.Client
	tableBucketARN string
}

func NewS3TablesAPI(cfg aws.Config, tableBucketARN string) *S3TablesAPI {
	return &S3TablesAPI{
		client:         s3tables.NewFromConfig(cfg),
		tableBucketARN: tableBucketARN,
	}
}

func (s S3TablesAPI) GetNamespace(ctx context.Context, namespace string) (s3tables.GetNamespaceOutput, error) {
	resp, err := s.client.GetNamespace(ctx, &s3tables.GetNamespaceInput{
		Namespace:      aws.String(namespace),
		TableBucketARN: aws.String(s.tableBucketARN),
	})

	if err != nil {
		return s3tables.GetNamespaceOutput{}, err
	}

	return *resp, nil
}

func (s S3TablesAPI) ListNamespaces(ctx context.Context) ([]types.NamespaceSummary, error) {
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
