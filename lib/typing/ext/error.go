package ext

import "errors"

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

func IsParseError(err error) bool {
	return errors.As(err, &ParseError{})
}
