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

type ParseErrorKind string

const (
	UnsupportedDateLayout ParseErrorKind = "unsupported_date_layout"
)

type ParseError struct {
	message string
	kind    ParseErrorKind
}

func NewParseError(message string, kind ParseErrorKind) ParseError {
	return ParseError{message: message, kind: kind}
}

func (p ParseError) Error() string {
	return p.message
}

func (p ParseError) GetKind() ParseErrorKind {
	return p.kind
}

func BuildParseError(err error) (ParseError, bool) {
	var parseError ParseError
	if errors.As(err, &parseError) {
		return parseError, true
	}

	return ParseError{}, false
}
