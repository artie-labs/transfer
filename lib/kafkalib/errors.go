package kafkalib

import (
	"errors"
	"fmt"
)

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

func (e FetchMessageError) Unwrap() error {
	return e.Err
}

func AsFetchMessageError(err error) (FetchMessageError, bool) {
	var fetchErr FetchMessageError
	return fetchErr, errors.As(err, &fetchErr)
}
