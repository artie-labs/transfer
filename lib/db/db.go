package db

import (
	"database/sql"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/logger"
)

const (
	maxAttempts = 3
	sleepBaseMs = 500
	sleepMaxMs  = 3500
)

type Store interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	Begin() (*sql.Tx, error)
}

type storeWrapper struct {
	*sql.DB
}

func (s *storeWrapper) Exec(query string, args ...any) (sql.Result, error) {
	var result sql.Result
	var err error
	for attempts := 0; attempts < maxAttempts; attempts++ {
		if attempts > 0 {
			sleepDuration := jitter.Jitter(sleepBaseMs, sleepMaxMs, attempts-1)
			slog.Warn("Failed to execute the query, retrying...",
				slog.Any("err", err),
				slog.Duration("sleep", sleepDuration),
				slog.Int("attempts", attempts),
			)
			time.Sleep(sleepDuration)
		}

		result, err = s.DB.Exec(query, args...)
		if err == nil || !retryableError(err) {
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

func Open(driverName, dsn string) Store {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		logger.Panic("Failed to start a SQL client",
			slog.String("driverName", driverName),
			slog.Any("err", err),
		)
	}

	err = db.Ping()
	if err != nil {
		logger.Panic("Failed to validate the DB connection",
			slog.String("driverName", driverName),
			slog.Any("err", err),
		)
	}

	return &storeWrapper{
		DB: db,
	}
}
