package awslib

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func NewConfigWithCredentialsAndRegion(ctx context.Context, credentials credentials.StaticCredentialsProvider, region string) aws.Config {
	return aws.Config{
		Region:      region,
		Credentials: credentials,
	}
}
