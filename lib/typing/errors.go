package typing

import "errors"

type UnsupportedDataTypeError struct {
	message string
}

func NewUnsupportedDataTypeError(message string) UnsupportedDataTypeError {
	return UnsupportedDataTypeError{message: message}
}

func (u UnsupportedDataTypeError) Error() string {
	return u.message
}

func IsUnsupportedDataTypeError(err error) bool {
	return errors.As(err, &UnsupportedDataTypeError{})
}
