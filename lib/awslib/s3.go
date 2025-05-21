package awslib

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func (s S3Client) DeleteFolder(ctx context.Context, bucket, folder string) error {
	var continuationToken *string
	for {
		objects, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(folder),
			ContinuationToken: continuationToken,
		})

		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		// If no objects found, we're done
		if len(objects.Contents) == 0 {
			return nil
		}

		var objectIdentifiers []types.ObjectIdentifier
		for _, object := range objects.Contents {
			objectIdentifiers = append(objectIdentifiers, types.ObjectIdentifier{
				Key: object.Key,
			})
		}

		// Delete objects in batch
		_, err = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: objectIdentifiers,
				Quiet:   aws.Bool(true),
			},
		})

		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}

		// Check if we've reached the end of the listing
		noMoreObjects := !*objects.IsTruncated

		// Check if we're stuck with the same continuation token
		var isStuck bool
		if continuationToken != nil && objects.NextContinuationToken != nil {
			isStuck = *continuationToken == *objects.NextContinuationToken
		}

		// Break if either condition is true
		if noMoreObjects || isStuck {
			break
		}

		continuationToken = objects.NextContinuationToken
	}

	return nil
}
