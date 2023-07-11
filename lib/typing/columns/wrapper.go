package columns

import (
	"context"

	"github.com/artie-labs/transfer/lib/sql"
)

type Wrapper struct {
	name        string
	escapedName string
}

func NewWrapper(ctx context.Context, col Column, args *sql.NameArgs) Wrapper {
	return Wrapper{
		name:        col.name,
		escapedName: col.Name(ctx, args),
	}
}

func (w *Wrapper) EscapedName() string {
	return w.escapedName
}

func (w *Wrapper) RawName() string {
	return w.name
}
