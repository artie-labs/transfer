package columns

type Wrapper struct {
	name        string
	escapedName string
}

func NewWrapper(col Column, args *NameArgs) Wrapper {
	return Wrapper{
		name:        col.name,
		escapedName: col.Name(args),
	}
}

func (w *Wrapper) EscapedName() string {
	return w.escapedName
}

func (w *Wrapper) RawName() string {
	return w.name
}
