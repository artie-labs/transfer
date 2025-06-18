package mocks

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o=db.store.mock.go ../db Store
//counterfeiter:generate -o=kafkalib.consumer.mock.go ../kafkalib Consumer

//counterfeiter:generate -o=destination.mock.go ../destination Destination
//counterfeiter:generate -o=baseline.mock.go ../destination Baseline
//counterfeiter:generate -o=tableid.mock.go ../sql TableIdentifier

//counterfeiter:generate -o=event.mock.go ../cdc Event
