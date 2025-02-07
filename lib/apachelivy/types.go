package apachelivy

import (
	"fmt"
	"strings"
)

// SessionState - https://livy.incubator.apache.org/docs/latest/rest-api.html#:~:text=of%20key%3Dval-,Session%20State,-Value
type SessionState string

const (
	StateNotStarted   SessionState = "not_started"
	StateStarting     SessionState = "starting"
	StateIdle         SessionState = "idle"
	StateBusy         SessionState = "busy"
	StateShuttingDown SessionState = "shutting_down"
	StateDead         SessionState = "dead"
	StateKilled       SessionState = "killed"
	StateSuccess      SessionState = "success"
	StateError        SessionState = "error"
)

func (s SessionState) IsReady() bool {
	return s == StateIdle
}

type GetSessionResponse struct {
	ID    int          `json:"id"`
	State SessionState `json:"state"`
	Kind  string       `json:"kind"`
}

type CreateSessionRequest struct {
	Kind string         `json:"kind"`
	Jars []string       `json:"jars,omitempty"`
	Conf map[string]any `json:"conf"`
}

type CreateSessionResponse struct {
	ID    int          `json:"id"`
	State SessionState `json:"state"`
}

type CreateStatementRequest struct {
	Code string `json:"code"`
	Kind string `json:"kind"`
}

type CreateStatementResponse struct {
	ID     int             `json:"id"`
	State  string          `json:"state"`
	Output StatementOutput `json:"output"`
}

type StatementOutput struct {
	Status    string                 `json:"status"`
	Data      map[string]interface{} `json:"data,omitempty"`
	EType     string                 `json:"etype,omitempty"`
	EValue    string                 `json:"evalue,omitempty"`
	TrackBack []string               `json:"trackback"`
}

type GetStatementResponse struct {
	ID        int `json:"id"`
	Code      string
	State     string          `json:"state"`
	Output    StatementOutput `json:"output"`
	Started   int             `json:"started"`
	Completed int             `json:"completed"`
}

func (a GetStatementResponse) Error(sessionID int) error {
	if a.Output.Status == "error" {
		return fmt.Errorf("statement: %d for session: %d failed: %s, stacktrace: %s", a.ID, sessionID, a.Output.EValue, strings.Join(a.Output.TrackBack, "\n"))
	}

	return nil
}
