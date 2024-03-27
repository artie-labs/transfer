package columns

import (
	"github.com/artie-labs/transfer/lib/sql"
)

type Wrapper struct {
	name        string
	escapedName string
}

func NewWrapper(col Column, uppercaseEscNames bool, args *sql.NameArgs) Wrapper {
	return Wrapper{
		name:        col.name,
		escapedName: col.Name(uppercaseEscNames, args),
	}
}

func (w Wrapper) EscapedName() string {
	return w.escapedName
}

func (w Wrapper) RawName() string {
	return w.name
}
