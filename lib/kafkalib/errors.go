package kafkalib

import "fmt"

type FetchMessageError struct {
	Err error
}

func NewFetchMessageError(err error) FetchMessageError {
	return FetchMessageError{
		Err: err,
	}
}

func (e FetchMessageError) Error() string {
	return fmt.Sprintf("failed to fetch message: %v", e.Err)
}

func IsFetchMessageError(err error) (FetchMessageError, bool) {
	fetchMessageError, ok := err.(FetchMessageError)
	return fetchMessageError, ok
}
