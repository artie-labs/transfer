package converters

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

type StringConverter interface {
	Convert(value any) (string, error)
}

func GetStringConverter(kd typing.KindDetails) (StringConverter, error) {
	switch kd.Kind {
	case typing.Date.Kind:
		return DateStringConverter{}, nil
	}

	return nil, nil
}

type DateStringConverter struct{}

func (DateStringConverter) Convert(value any) (string, error) {
	_time, err := ext.ParseDateFromAny(value)
	if err != nil {
		return "", fmt.Errorf("failed to cast colVal as date, colVal: '%v', err: %w", value, err)
	}

	return _time.Format(ext.PostgresDateFormat), nil
}
