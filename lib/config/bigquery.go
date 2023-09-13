package config

import "fmt"

type BigQuery struct {
	// PathToCredentials is _optional_ if you have GOOGLE_APPLICATION_CREDENTIALS set as an env var
	// Links to credentials: https://cloud.google.com/docs/authentication/application-default-credentials#GAC
	PathToCredentials string `yaml:"pathToCredentials"`
	DefaultDataset    string `yaml:"defaultDataset"`
	ProjectID         string `yaml:"projectID"`
	Location          string `yaml:"location"`
	BatchSize         int    `yaml:"batchSize"`
}

func (b *BigQuery) LoadDefaultValues() {
	if b.BatchSize == 0 {
		b.BatchSize = 1000
	}
}

// DSN - returns the notation for BigQuery following this format: bigquery://projectID/[location/]datasetID?queryString
// If location is passed in, we'll specify it. Else, it'll default to empty and our library will set it to US.
func (b *BigQuery) DSN() string {
	dsn := fmt.Sprintf("bigquery://%s/%s", b.ProjectID, b.DefaultDataset)

	if b.Location != "" {
		dsn = fmt.Sprintf("bigquery://%s/%s/%s", b.ProjectID, b.Location, b.DefaultDataset)
	}

	return dsn
}
