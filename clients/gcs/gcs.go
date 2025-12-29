package gcs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/artie-labs/transfer/clients/s3"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/gcslib"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
)

const GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"

type Store struct {
	config    config.Config
	gcsClient gcslib.GCSClient
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) Validate() error {
	if err := s.config.GCS.Validate(); err != nil {
		return fmt.Errorf("failed to validate settings: %w", err)
	}

	return nil
}

func (s *Store) IdentifierFor(topicConfig kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table, s.config.GCS.TableNameSeparator)
}

// ObjectPrefix - this will generate the exact right prefix that we need to write into GCS.
// It will look like something like this:
// > folderName/fullyQualifiedTableName/YYYY-MM-DD
func (s *Store) ObjectPrefix(tableData *optimization.TableData) string {
	tableID := s.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	fqTableName := tableID.FullyQualifiedName()
	// Adding date= prefix so that it adheres to the partitioning format for Hive.
	yyyyMMDDFormat := fmt.Sprintf("date=%s", time.Now().Format(time.DateOnly))
	if len(s.config.GCS.FolderName) > 0 {
		return strings.Join([]string{s.config.GCS.FolderName, fqTableName, yyyyMMDDFormat}, "/")
	}

	return strings.Join([]string{fqTableName, yyyyMMDDFormat}, "/")
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, _ bool) error {
	// There's no difference in appending or merging for GCS.
	if _, err := s.Merge(ctx, tableData, whClient); err != nil {
		return fmt.Errorf("failed to merge: %w", err)
	}

	return nil
}

func buildTemporaryFilePath(tableData *optimization.TableData) string {
	return fmt.Sprintf("/tmp/%d_%s.parquet", tableData.GetLatestTimestamp().UnixMilli(), stringutil.Random(4))
}

// Merge - will take tableData, write it into a particular file in the specified format, in these steps:
// 1. Load a ParquetWriter from a JSON schema (auto-generated)
// 2. Load the temporary file, under this format: gs://bucket/folderName/fullyQualifiedTableName/YYYY-MM-DD/{{unix_timestamp}}.parquet
// 3. It will then upload this to GCS
// 4. Delete the temporary file
func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client) (bool, error) {
	if tableData.ShouldSkipUpdate() {
		return false, nil
	}

	fp := buildTemporaryFilePath(tableData)
	if err := s3.WriteParquetFiles(tableData, fp); err != nil {
		return false, err
	}

	defer func() {
		// Delete the file regardless of outcome to avoid fs build up.
		if removeErr := os.RemoveAll(fp); removeErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", removeErr), slog.String("filePath", fp))
		}
	}()

	gcsPath, err := s.gcsClient.UploadLocalFileToGCS(ctx, s.config.GCS.Bucket, s.ObjectPrefix(tableData), fp)
	if err != nil {
		return false, fmt.Errorf("failed to upload file to GCS: %w", err)
	}

	slog.Info("Successfully wrote and uploaded Parquet file to GCS", slog.String("filePath", fp), slog.String("gcsPath", gcsPath))
	return true, nil
}

func (s *Store) IsRetryableError(_ error) bool {
	return false // not supported for GCS
}

func (s *Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	castedTableID, ok := tableID.(TableIdentifier)
	if !ok {
		return fmt.Errorf("expected tableID to be a TableIdentifier, got %T", tableID)
	}

	return s.gcsClient.DeleteFolder(ctx, s.config.GCS.Bucket, castedTableID.FullyQualifiedName())
}

func LoadStore(ctx context.Context, cfg config.Config) (*Store, error) {
	if credPath := cfg.GCS.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		slog.Debug("Writing the path to GCS credentials to env var for google auth")
		if err := os.Setenv(GooglePathToCredentialsEnvKey, credPath); err != nil {
			return nil, fmt.Errorf("error setting env var for %q: %w", GooglePathToCredentialsEnvKey, err)
		}
	}

	var opts []option.ClientOption
	if envCreds := os.Getenv(GooglePathToCredentialsEnvKey); envCreds != "" {
		opts = append(opts, option.WithCredentialsFile(envCreds))
	}

	storageClient, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	store := Store{
		config:    cfg,
		gcsClient: gcslib.NewGCSClient(ctx, storageClient),
	}

	if err := store.Validate(); err != nil {
		return nil, err
	}

	return &store, nil
}
