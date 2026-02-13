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
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Conn(ctx context.Context) (*sql.Conn, error)
	Begin(ctx context.Context) (*sql.Tx, error)
	IsRetryableError(err error) bool
	GetDatabase() *sql.DB
	Close() error
}

type storeWrapper struct {
	*sql.DB
}

func NewStoreWrapper(db *sql.DB) Store {
	return &storeWrapper{
		DB: db,
	}
}

func (s *storeWrapper) Close() error {
	return s.DB.Close()
}

func (s *storeWrapper) GetDatabase() *sql.DB {
	return s.DB
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

func (s *storeWrapper) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.DB.QueryContext(ctx, query, args...)
}

func (s *storeWrapper) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return s.DB.QueryRowContext(ctx, query, args...)
}

func (s *storeWrapper) Begin(ctx context.Context) (*sql.Tx, error) {
	return s.DB.BeginTx(ctx, nil)
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

func WithDatabase(db *sql.DB) (Store, error) {
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to validate the DB connection: %w", err)
	}

	return &storeWrapper{DB: db}, nil
}
