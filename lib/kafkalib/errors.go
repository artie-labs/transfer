package kafkalib

import "fmt"

type FetchMessageError struct {
	msg string
}

func NewFetchMessageError(err error) FetchMessageError {
	return FetchMessageError{
		msg: err.Error(),
	}
}

func (e FetchMessageError) Error() string {
	return fmt.Sprintf("failed to fetch message: %q", e.msg)
}

func IsFetchMessageError(err error) bool {
	_, ok := err.(FetchMessageError)
	return ok
}
