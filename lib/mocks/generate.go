package mocks

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o=db.store.mock.go ../db Store
//counterfeiter:generate -o=kafkalib.consumer.mock.go ../kafkalib Consumer
//counterfeiter:generate -o=bigquery.client.mock.go ../../clients/bigquery/clients Client
