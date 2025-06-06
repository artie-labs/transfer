package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

func (s *storeWrapper) ExecContextStatements(ctx context.Context, statements []string) ([]sql.Result, error) {
	switch len(statements) {
	case 0:
		return nil, fmt.Errorf("statements is empty")
	case 1:
		slog.Debug("Executing...", slog.String("query", statements[0]))
		result, err := s.ExecContext(ctx, statements[0])
		if err != nil {
			return nil, fmt.Errorf("failed to execute statement: %w", err)
		}

		return []sql.Result{result}, nil
	default:
		tx, err := s.Begin()
		if err != nil {
			return nil, fmt.Errorf("failed to start tx: %w", err)
		}
		var committed bool
		defer func() {
			if !committed {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					slog.Warn("Unable to rollback", slog.Any("err", rollbackErr))
				}
			}
		}()

		var results []sql.Result
		for _, statement := range statements {
			slog.Debug("Executing...", slog.String("query", statement))
			result, err := tx.ExecContext(ctx, statement)
			if err != nil {
				return nil, fmt.Errorf("failed to execute statement: %q, err: %w", statement, err)
			}

			results = append(results, result)
		}

		if err = tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit statements: %v, err: %w", statements, err)
		}
		committed = true
		return results, nil
	}
}
