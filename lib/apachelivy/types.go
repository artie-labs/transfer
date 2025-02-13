package apachelivy

import (
	"encoding/json"
	"fmt"
	"strings"
)

// SessionKind - https://livy.incubator.apache.org/docs/latest/rest-api.html#session-kind
type SessionKind string

const (
	SessionKindSpark   SessionKind = "spark"
	SessionKindPySpark SessionKind = "pyspark"
	SessionKindSparkR  SessionKind = "sparkr"
	SessionKindSql     SessionKind = "sql"
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

type DescribeSchemaResponse struct {
	Type  string  `json:"type"`
	Field []Field `json:"field"`
	Data  []any   `json:"data"`
}

type FieldName string

const (
	ColumnNameField FieldName = "column_name"
	DataTypeField   FieldName = "data_type"
	CommentField    FieldName = "comment"
)

type Field struct {
	Name FieldName `json:"name"`
	Type string    `json:"type"`
}

type CreateStatementResponse struct {
	ID     int             `json:"id"`
	State  string          `json:"state"`
	Output StatementOutput `json:"output"`
}

type StatementOutput struct {
	Status    string         `json:"status"`
	Data      map[string]any `json:"data,omitempty"`
	EType     string         `json:"etype,omitempty"`
	EValue    string         `json:"evalue,omitempty"`
	TraceBack []string       `json:"traceback"`
}

type GetStatementResponse struct {
	ID        int             `json:"id"`
	Code      string          `json:"code"`
	State     string          `json:"state"`
	Output    StatementOutput `json:"output"`
	Started   int             `json:"started"`
	Completed int             `json:"completed"`
}

func (g GetStatementResponse) Error(sessionID int) error {
	if g.Output.Status == "error" {
		return fmt.Errorf("%s, stacktrace: %s for session %d, statement %d", g.Output.EValue, strings.Join(g.Output.TraceBack, "\n"), sessionID, g.ID)
	}

	return nil
}

func (g GetStatementResponse) MarshalJSON() ([]byte, error) {
	if g.Output.Data == nil {
		return nil, fmt.Errorf("data is nil")
	}

	jsonData, ok := g.Output.Data["application/json"]
	if !ok {
		return nil, fmt.Errorf("data is not application/json")
	}

	return json.Marshal(jsonData)
}

type GetSchemaResponse struct {
	Schema GetSchemaStructResponse `json:"schema"`
	Data   [][]string              `json:"data"`
}

type GetSchemaStructResponse struct {
	Fields []GetSchemaFieldResponse `json:"fields"`
}

type GetSchemaFieldResponse struct {
	Name     string         `json:"name"`
	Type     string         `json:"type"`
	Nullable bool           `json:"nullable"`
	Metadata map[string]any `json:"metadata"`
}
