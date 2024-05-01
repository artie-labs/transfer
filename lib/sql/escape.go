package sql

func EscapeNameIfNecessary(name string, dialect Dialect) string {
	if dialect.NeedsEscaping(name) {
		return dialect.QuoteIdentifier(name)
	}
	return name
}
