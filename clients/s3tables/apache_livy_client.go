package s3tables

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/artie-labs/transfer/lib/config"
)

type Client struct {
	url         string
	sessionID   int
	httpClient  *http.Client
	sessionConf map[string]any
}

func (c Client) ExecContext(ctx context.Context, query string) error {
	_, err := c.submitLivyStatement(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (c Client) submitLivyStatement(ctx context.Context, code string) (int, error) {
	reqBody, err := json.Marshal(ApacheLivyCreateStatementRequest{Code: code})
	if err != nil {
		return 0, err
	}

	respBytes, err := c.doRequest(ctx, "POST", fmt.Sprintf("/sessions/%d/statements", c.sessionID), reqBody)
	if err != nil {
		return 0, err
	}

	var resp ApacheLivyCreateStatementResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return 0, err
	}

	return resp.ID, nil
}

func (c Client) doRequest(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.url+path, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code when creating session: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (c *Client) newSession(ctx context.Context, kind string) error {
	body, err := json.Marshal(ApacheLivyCreateSessionRequest{
		Kind: kind,
		Conf: c.sessionConf,
	})
	if err != nil {
		return err
	}

	resp, err := c.doRequest(ctx, "POST", "/sessions", body)
	if err != nil {
		slog.Warn("Failed to create session", slog.Any("err", err), slog.String("response", string(resp)))
		return err
	}

	var createResp ApacheLivyCreateSessionResponse
	if err = json.Unmarshal(resp, &createResp); err != nil {
		return err
	}

	c.sessionID = createResp.ID
	return nil
}

func NewClient(ctx context.Context, cfg config.Config) (Client, error) {
	client := Client{
		url:        cfg.S3Tables.ApacheLivyURL,
		httpClient: &http.Client{},
		sessionConf: map[string]any{
			"spark.sql.catalog.s3tablesbucket":              "org.apache.iceberg.spark.SparkCatalog",
			"spark.sql.catalog.s3tablesbucket.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
			"spark.sql.catalog.s3tablesbucket.warehouse":    cfg.S3Tables.BucketARN,
			"spark.sql.extensions":                          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
			"spark.hadoop.fs.s3a.access.key":                cfg.S3Tables.AwsAccessKeyID,
			"spark.hadoop.fs.s3a.secret.key":                cfg.S3Tables.AwsSecretAccessKey,
		},
	}

	// https://livy.incubator.apache.org/docs/latest/rest-api.html#session-kind
	if err := client.newSession(ctx, "sql"); err != nil {
		return Client{}, err
	}

	return client, nil
}
