package s3tables

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/artie-labs/transfer/lib/config"
)

type Client struct {
	url         string
	sessionID   int
	httpClient  *http.Client
	sessionConf map[string]any
	sessionJars []string
}

func (c Client) ExecContext(ctx context.Context, query string) error {
	statementID, err := c.submitLivyStatement(ctx, query)
	if err != nil {
		return err
	}

	resp, err := c.waitForStatement(ctx, statementID)
	if err != nil {
		return err
	}

	return resp.Error(c.sessionID)
}

func (c Client) waitForStatement(ctx context.Context, statementID int) (ApacheLivyGetStatementResponse, error) {
	// TODO: Add a timeout
	for {
		time.Sleep(1 * time.Second)
		respBytes, err := c.doRequest(ctx, "GET", fmt.Sprintf("/sessions/%d/statements/%d", c.sessionID, statementID), nil)
		if err != nil {
			return ApacheLivyGetStatementResponse{}, err
		}

		var resp ApacheLivyGetStatementResponse
		if err := json.Unmarshal(respBytes, &resp); err != nil {
			return ApacheLivyGetStatementResponse{}, err
		}

		if resp.Completed > 0 {
			return resp, nil
		}
	}
}

func (c Client) submitLivyStatement(ctx context.Context, code string) (int, error) {
	reqBody, err := json.Marshal(ApacheLivyCreateStatementRequest{Code: code, Kind: "sql"})
	if err != nil {
		return 0, err
	}

	respBytes, err := c.doRequest(ctx, "POST", fmt.Sprintf("/sessions/%d/statements", c.sessionID), reqBody)
	if err != nil {
		fmt.Println("string(respBytes)", string(respBytes))
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

	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return out, nil
}

func (c *Client) newSession(ctx context.Context, kind string) error {
	body, err := json.Marshal(ApacheLivyCreateSessionRequest{
		Kind: kind,
		Jars: c.sessionJars,
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

	fmt.Println("response", string(resp))

	c.sessionID = createResp.ID
	return nil
}

/*
spark-shell \
  --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
  --conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:us-west-2:xxx:bucket/test \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

*/

func NewClient(ctx context.Context, cfg config.Config) (Client, error) {
	client := Client{
		url:         cfg.S3Tables.ApacheLivyURL,
		httpClient:  &http.Client{},
		sessionJars: []string{"local:/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar", "local:/opt/spark/jars/s3-tables-catalog-for-iceberg-0.1.4.jar", "local:/opt/spark/jars/s3tables-2.30.14.jar"},
		sessionConf: map[string]any{
			"spark.driver.extraJavaOptions":   fmt.Sprintf("-Daws.accessKeyId=%s -Daws.secretAccessKey=%s", cfg.S3Tables.AwsAccessKeyID, cfg.S3Tables.AwsSecretAccessKey),
			"spark.executor.extraJavaOptions": fmt.Sprintf("-Daws.accessKeyId=%s -Daws.secretAccessKey=%s", cfg.S3Tables.AwsAccessKeyID, cfg.S3Tables.AwsSecretAccessKey),
			// iceberg-spark-runtime-3.2_2.12-1.4.3
			"spark.jars.packages":                            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4",
			"spark.sql.extensions":                           "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
			"spark.sql.catalog.s3tablesbucket":               "org.apache.iceberg.spark.SparkCatalog",
			"spark.sql.catalog.s3tablesbucket.catalog-impl":  "software.amazon.s3tables.iceberg.S3TablesCatalog",
			"spark.sql.catalog.s3tablesbucket.warehouse":     cfg.S3Tables.BucketARN,
			"spark.sql.catalog.s3tablesbucket.client.region": cfg.S3Tables.Region,
		},
	}

	// https://livy.incubator.apache.org/docs/latest/rest-api.html#session-kind
	if err := client.newSession(ctx, "sql"); err != nil {
		return Client{}, err
	}

	time.Sleep(10 * time.Second)

	return client, nil
}
