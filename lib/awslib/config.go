package awslib

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func NewConfigWithCredentialsAndRegion(credentials credentials.StaticCredentialsProvider, region string) aws.Config {
	return aws.Config{
		Region:      region,
		Credentials: credentials,
	}
}

func NewDefaultConfig(ctx context.Context, region string) (aws.Config, error) {
	return config.LoadDefaultConfig(ctx, config.WithRegion(region))
}
