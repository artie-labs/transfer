package db

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/logger"
)

const (
	maxAttempts     = 3
	sleepIntervalMs = 500
)

type Store interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	Begin() (*sql.Tx, error)
}

type storeWrapper struct {
	*sql.DB
	ctx context.Context
}

func (s *storeWrapper) Exec(query string, args ...any) (sql.Result, error) {
	var result sql.Result
	var err error
	for attempts := 0; attempts < maxAttempts; attempts++ {
		result, err = s.DB.Exec(query, args...)
		if err == nil {
			break
		}

		if retryableError(err) {
			sleepDurationMs := jitter.JitterMs(sleepIntervalMs, attempts)
			slog.Warn("failed to execute the query, retrying...",
				slog.Any("err", err),
				slog.Int("sleepDurationMs", sleepDurationMs),
				slog.Int("attempts", attempts),
			)

			time.Sleep(time.Duration(sleepDurationMs) * time.Millisecond)
			continue
		}

		break
	}
	return result, err
}

func (s *storeWrapper) Query(query string, args ...any) (*sql.Rows, error) {
	return s.DB.Query(query, args...)
}

func (s *storeWrapper) Begin() (*sql.Tx, error) {
	return s.DB.Begin()
}

func Open(ctx context.Context, driverName, dsn string) Store {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		logger.Fatal("Failed to start a SQL client",
			slog.String("driverName", driverName),
			slog.Any("err", err),
		)
	}

	err = db.Ping()
	if err != nil {
		logger.Fatal("Failed to validate the DB connection",
			slog.String("driverName", driverName),
			slog.Any("err", err),
		)
	}

	return &storeWrapper{
		DB:  db,
		ctx: ctx,
	}
}
