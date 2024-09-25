package config

import "fmt"

// DSN - returns the notation for BigQuery following this format: bigquery://projectID/[location/]datasetID?queryString
// If location is passed in, we'll specify it. Else, it'll default to empty and our library will set it to US.
func (b *BigQuery) DSN() string {
	dsn := fmt.Sprintf("bigquery://%s/%s", b.ProjectID, b.DefaultDataset)

	if b.Location != "" {
		dsn = fmt.Sprintf("bigquery://%s/%s/%s", b.ProjectID, b.Location, b.DefaultDataset)
	}

	return dsn
}
