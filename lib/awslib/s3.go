package awslib

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type UploadArgs struct {
	Bucket                     string
	OptionalS3Prefix           string
	FilePath                   string
	OverrideAWSAccessKeyID     string
	OverrideAWSAccessKeySecret string
	OverrideAWSSessionToken    string
	Region                     string
}

// UploadLocalFileToS3 - takes a filepath with the file and bucket and optional expiry
// It will then upload it and then return the S3 URI and any error(s).
func UploadLocalFileToS3(ctx context.Context, args UploadArgs) (string, error) {
	var cfg aws.Config
	var err error

	if args.OverrideAWSAccessKeyID != "" && args.OverrideAWSAccessKeySecret != "" {
		creds := credentials.NewStaticCredentialsProvider(args.OverrideAWSAccessKeyID, args.OverrideAWSAccessKeySecret, args.OverrideAWSSessionToken)
		cfg, err = config.LoadDefaultConfig(ctx, config.WithCredentialsProvider(creds), config.WithRegion(args.Region))
	} else {
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(args.Region))
	}

	if err != nil {
		return "", fmt.Errorf("failed loading s3 config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	file, err := os.Open(args.FilePath)
	if err != nil {
		return "", err
	}

	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		return "", err
	}

	objectKey := fileInfo.Name()
	if args.OptionalS3Prefix != "" {
		objectKey = fmt.Sprintf("%s/%s", args.OptionalS3Prefix, objectKey)
	}

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(args.Bucket),
		Key:    aws.String(objectKey),
		Body:   file,
	})

	if err != nil {
		return "", err
	}

	return fmt.Sprintf("s3://%s/%s", args.Bucket, objectKey), nil
}
