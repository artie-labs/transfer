package s3tables

import (
	"fmt"
	"strings"
)

type ApacheLivyCreateSessionRequest struct {
	Kind string         `json:"kind"`
	Jars []string       `json:"jars,omitempty"`
	Conf map[string]any `json:"conf"`
}

type ApacheLivyCreateSessionResponse struct {
	ID    int    `json:"id"`
	State string `json:"state"`
}

type ApacheLivyCreateStatementRequest struct {
	Code string `json:"code"`
	Kind string `json:"kind"`
}

type ApacheLivyCreateStatementResponse struct {
	ID     int                       `json:"id"`
	State  string                    `json:"state"`
	Output ApacheLivyStatementOutput `json:"output"`
}

type ApacheLivyStatementOutput struct {
	Status    string                 `json:"status"`
	Data      map[string]interface{} `json:"data,omitempty"`
	EType     string                 `json:"etype,omitempty"`
	EValue    string                 `json:"evalue,omitempty"`
	TrackBack []string               `json:"trackback"`
}

type ApacheLivyGetStatementResponse struct {
	ID        int `json:"id"`
	Code      string
	State     string                    `json:"state"`
	Output    ApacheLivyStatementOutput `json:"output"`
	Started   int                       `json:"started"`
	Completed int                       `json:"completed"`
}

func (a ApacheLivyGetStatementResponse) Error(sessionID int) error {
	if a.Output.Status == "error" {
		return fmt.Errorf("statement: %d for session: %d failed: %s, stacktrace: %s", a.ID, sessionID, a.Output.EValue, strings.Join(a.Output.TrackBack, "\n"))
	}

	return nil
}
