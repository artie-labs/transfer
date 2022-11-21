package mock

import (
	"database/sql"
	"fmt"

	"github.com/artie-labs/transfer/lib/mocks"
)

// DB is used for testing purposes. It will log the resulting command,
// so we can see what is being executed.
type DB struct {
	Fake mocks.FakeStore
}

func (m *DB) Exec(query string, args ...any) (sql.Result, error) {
	fmt.Println("Mock DB is executing", "query", query, "args", args)
	return m.Fake.Exec(query, args)
}

func (m *DB) Query(query string, args ...any) (*sql.Rows, error) {
	fmt.Println("Mock DB is querying", "query", query, "args", args)
	return m.Fake.Query(query, args)
}
