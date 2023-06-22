package s3

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type UploadArgs struct {
	Bucket   string
	FilePath string
	// If expiry is set, we'll set a 6h (default) expiration timestamp.
	Expiry bool
}

// UploadLocalFileToS3 - takes a filepath with the file and bucket and optional expiry
// It will then upload it and then return the S3 URI and any error(s).
func UploadLocalFileToS3(ctx context.Context, args UploadArgs) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", err
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

	objectKey := fmt.Sprintf("uploaded_files/%s", fileInfo.Name())
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(args.Bucket),
		Key:    aws.String(objectKey),
		Body:   file,
	})

	if err != nil {
		return "", err
	}

	if args.Expiry {
		// Set the object expiration to 6 hours from now
		expiration := time.Now().Add(6 * time.Hour)
		ruleID := "ExpireRule"
		rule := types.LifecycleRule{
			ID:     &ruleID,
			Status: types.ExpirationStatusEnabled,
			Expiration: &types.LifecycleExpiration{
				Date: &expiration,
			},
		}

		input := &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(args.Bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{rule},
			},
		}

		_, err = s3Client.PutBucketLifecycleConfiguration(ctx, input)
		if err != nil {
			return "", fmt.Errorf("failed applying lifecycle rule: %v", err)
		}

		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("s3://%s/%s", args.Bucket, objectKey), nil
}
