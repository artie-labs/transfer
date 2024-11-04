package converters

import (
	"github.com/artie-labs/transfer/lib/typing"
)

type ValueConverter interface {
	ToKindDetails() typing.KindDetails
	Convert(value any) (any, error)
}
