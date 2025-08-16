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

func IsFetchMessageError(err error) bool {
	_, ok := err.(FetchMessageError)
	return ok
}
