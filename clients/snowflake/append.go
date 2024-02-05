package snowflake

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData) error {
	err := s.append(ctx, tableData)
	if IsAuthExpiredError(err) {
		slog.Warn("authentication has expired, will reload the Snowflake store and retry appending", slog.Any("err", err))
		s.reestablishConnection()
		return s.Append(ctx, tableData)
	}

	return err
}

func (s *Store) append(ctx context.Context, tableData *optimization.TableData) error {
	return fmt.Errorf("snowflake: %s did not implement this yet", s.Label())
}
