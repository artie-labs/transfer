package awslib

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client struct {
	cfg    aws.Config
	client *s3.Client
}

func NewS3Client(cfg aws.Config) S3Client {
	return S3Client{
		cfg:    cfg,
		client: s3.NewFromConfig(cfg),
	}
}

func (s S3Client) UploadLocalFileToS3(ctx context.Context, bucket, prefix, filepath string) (string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}

	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("failed to get file info: %w", err)
	}

	objectKey := fileInfo.Name()
	if prefix != "" {
		objectKey = fmt.Sprintf("%s/%s", prefix, objectKey)
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
		Body:   file,
	})

	if err != nil {
		return "", fmt.Errorf("failed to upload file to s3: %w", err)
	}

	return fmt.Sprintf("s3://%s/%s", bucket, objectKey), nil
}
