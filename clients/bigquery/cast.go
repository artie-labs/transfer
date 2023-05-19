package bigquery

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/array"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/typing"
)

func CastColVal(colVal interface{}, colKind typing.Column) (string, error) {
	if colVal != nil {
		switch colKind.KindDetails.Kind {
		case typing.ETime.Kind:
			extTime, err := ext.ParseFromInterface(colVal)
			if err != nil {
				return "", fmt.Errorf("failed to cast colVal as time.Time, colVal: %v, err: %v", colVal, err)
			}

			switch extTime.NestedKind.Type {
			case ext.DateTimeKindType:
				colVal = fmt.Sprintf("PARSE_DATETIME('%s', '%v')", RFC3339Format, extTime.String(time.RFC3339Nano))
			case ext.DateKindType:
				colVal = fmt.Sprintf("PARSE_DATE('%s', '%v')", PostgresDateFormat, extTime.String(ext.Date.Format))
			case ext.TimeKindType:
				colVal = fmt.Sprintf("PARSE_TIME('%s', '%v')", PostgresTimeFormatNoTZ, extTime.String(ext.PostgresTimeFormatNoTZ))
			}
		// All the other types do not need string wrapping.
		case typing.String.Kind, typing.Struct.Kind:
			colVal = stringutil.Wrap(colVal)
			colVal = stringutil.LineBreaksToCarriageReturns(fmt.Sprint(colVal))
			if colKind.KindDetails == typing.Struct {
				if strings.Contains(fmt.Sprint(colVal), constants.ToastUnavailableValuePlaceholder) {
					colVal = typing.BigQueryJSON(fmt.Sprintf(`{"key": "%s"}`, constants.ToastUnavailableValuePlaceholder))
				} else {
					// This is how you cast string -> JSON
					colVal = fmt.Sprintf("JSON %s", colVal)
				}
			}
		case typing.Array.Kind:
			var err error
			colVal, err = array.InterfaceToArrayStringEscaped(colVal)
			if err != nil {
				return "", err
			}
		}
	} else {
		if colKind.KindDetails == typing.String {
			// BigQuery does not like null as a string for CTEs.
			// It throws this error: Value of type INT64 cannot be assigned to column name, which has type STRING
			colVal = "''"
		} else {
			colVal = "null"
		}
	}

	return fmt.Sprint(colVal), nil
}
