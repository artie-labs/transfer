package s3tables

type ApacheLivyCreateSessionRequest struct {
	Kind string         `json:"kind"`
	Conf map[string]any `json:"conf"`
}

type ApacheLivyCreateSessionResponse struct {
	ID    int    `json:"id"`
	State string `json:"state"`
}

type ApacheLivyCreateStatementRequest struct {
	Code string `json:"code"`
}

type ApacheLivyCreateStatementResponse struct {
	ID     int                       `json:"id"`
	State  string                    `json:"state"`
	Output ApacheLivyStatementOutput `json:"output"`
}

type ApacheLivyStatementOutput struct {
	Status string                 `json:"status"`
	Data   map[string]interface{} `json:"data,omitempty"`
	EType  string                 `json:"etype,omitempty"`
	EValue string                 `json:"evalue,omitempty"`
}
