package columns

import "github.com/artie-labs/transfer/lib/config/constants"

type Wrapper struct {
	name        string
	escapedName string
}

func NewWrapper(col Column, destKind constants.DestinationKind) Wrapper {
	return Wrapper{
		name:        col.name,
		escapedName: col.Name(destKind),
	}
}

func (w Wrapper) EscapedName() string {
	return w.escapedName
}

func (w Wrapper) RawName() string {
	return w.name
}
