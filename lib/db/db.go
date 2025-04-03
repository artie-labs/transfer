package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
)

const (
	maxAttempts = 5
	sleepBaseMs = 500
)

type Store interface {
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	Begin() (*sql.Tx, error)
	IsRetryableError(err error) bool
}

type storeWrapper struct {
	*sql.DB
}

func (s *storeWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	var err error
	for attempts := 0; attempts < maxAttempts; attempts++ {
		if attempts > 0 {
			sleepDuration := jitter.Jitter(sleepBaseMs, jitter.DefaultMaxMs, attempts-1)
			slog.Warn("Failed to execute the query, retrying...",
				slog.Any("err", err),
				slog.Duration("sleep", sleepDuration),
				slog.Int("attempts", attempts),
			)
			time.Sleep(sleepDuration)
		}

		result, err = s.DB.ExecContext(ctx, query, args...)
		if err == nil || !s.IsRetryableError(err) {
			break
		}
	}
	return result, err
}

func (s *storeWrapper) Exec(query string, args ...any) (sql.Result, error) {
	var result sql.Result
	var err error
	for attempts := 0; attempts < maxAttempts; attempts++ {
		if attempts > 0 {
			sleepDuration := jitter.Jitter(sleepBaseMs, jitter.DefaultMaxMs, attempts-1)
			slog.Warn("Failed to execute the query, retrying...",
				slog.Any("err", err),
				slog.Duration("sleep", sleepDuration),
				slog.Int("attempts", attempts),
			)
			time.Sleep(sleepDuration)
		}

		result, err = s.DB.Exec(query, args...)
		if err == nil || !s.IsRetryableError(err) {
			break
		}
	}
	return result, err
}

func (s *storeWrapper) Query(query string, args ...any) (*sql.Rows, error) {
	return s.DB.Query(query, args...)
}

func (s *storeWrapper) Begin() (*sql.Tx, error) {
	return s.DB.Begin()
}

func (s *storeWrapper) IsRetryableError(err error) bool {
	return isRetryableError(err)
}

func Open(driverName, dsn string) (Store, error) {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to start a SQL client for driver %q: %w", driverName, err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to validate the DB connection for driver %q: %w", driverName, err)
	}

	return &storeWrapper{
		DB: db,
	}, nil
}
