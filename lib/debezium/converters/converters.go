package converters

import (
	"github.com/artie-labs/transfer/lib/typing"
)

type ValueConverter interface {
	ToKindDetails() (typing.KindDetails, error)
	Convert(value any) (any, error)
}
