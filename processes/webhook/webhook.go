package webhook

import (
	"bytes"
	"encoding/json"
	"net/http"
)

type Status string

const (
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"
)

type Action struct {
	url      string
	metadata map[string]any
	status   Status
}

func (a Action) BuildPayload() (*http.Request, error) {
	payload := map[string]any{
		"url":      a.url,
		"metadata": a.metadata,
		"status":   a.status,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", a.url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	return req, nil
}
