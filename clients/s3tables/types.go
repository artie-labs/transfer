package s3tables

type ApacheLivyCreateSessionRequest struct {
	Kind string `json:"kind"`
}

type ApacheLivyCreateSessionResponse struct {
	ID    int    `json:"id"`
	State string `json:"state"`
}
