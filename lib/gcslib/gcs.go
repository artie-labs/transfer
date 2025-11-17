package gcslib

import (
	"context"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type GCSClient struct {
	client *storage.Client
}

func NewGCSClient(ctx context.Context, client *storage.Client) GCSClient {
	return GCSClient{
		client: client,
	}
}

func (g GCSClient) UploadLocalFileToGCS(ctx context.Context, bucket, prefix, filepath string) (string, error) {
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

	bkt := g.client.Bucket(bucket)
	obj := bkt.Object(objectKey)
	writer := obj.NewWriter(ctx)

	if _, err := io.Copy(writer, file); err != nil {
		writer.Close()
		return "", fmt.Errorf("failed to write file to GCS: %w", err)
	}

	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("failed to close GCS writer: %w", err)
	}

	return fmt.Sprintf("gs://%s/%s", bucket, objectKey), nil
}

// DeleteFolder - Folders in GCS are virtual, so we need to list all the objects in the folder and then delete them
func (g GCSClient) DeleteFolder(ctx context.Context, bucket, folder string) error {
	bkt := g.client.Bucket(bucket)
	query := &storage.Query{Prefix: folder}

	it := bkt.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		if err := bkt.Object(attrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete object %q: %w", attrs.Name, err)
		}
	}

	return nil
}

