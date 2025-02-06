package s3tables

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Client struct {
	url        string
	sessionID  int
	httpClient *http.Client
}

func (c Client) doRequest(method, path string, body []byte) ([]byte, error) {
	req, err := http.NewRequest(method, c.url+path, bytes.NewBuffer(body))
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

func (c *Client) newSession(kind string) error {
	body, err := json.Marshal(ApacheLivyCreateSessionRequest{Kind: kind})
	if err != nil {
		return err
	}

	resp, err := c.doRequest("POST", "/sessions", body)
	if err != nil {
		return err
	}

	var createResp ApacheLivyCreateSessionResponse
	if err = json.Unmarshal(resp, &createResp); err != nil {
		return err
	}

	c.sessionID = createResp.ID
	return nil
}

func NewClient(url string) (Client, error) {
	client := Client{url: url, httpClient: &http.Client{}}
	if err := client.newSession("artie-transfer"); err != nil {
		return Client{}, err
	}

	return client, nil
}
