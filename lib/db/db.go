package db

import (
	"context"
	"database/sql"
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
			sleepDuration := jitter.JitterMs(sleepIntervalMs, attempts)
			logger.FromContext(s.ctx).WithError(err).WithFields(map[string]interface{}{
				"sleepDuration (ms)": sleepDuration,
				"attempts":           attempts,
			}).Warn("failed to execute the query, retrying...")

			time.Sleep(sleepDuration * time.Millisecond)
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
		logger.FromContext(ctx).WithFields(map[string]interface{}{
			"driverName": driverName,
			"dsn":        dsn,
			"error":      err,
		}).Fatal("Failed to start a SQL client")
	}

	err = db.Ping()
	if err != nil {
		logger.FromContext(ctx).WithFields(map[string]interface{}{
			"driverName": driverName,
			"dsn":        dsn,
			"error":      err,
		}).Fatal("Failed to validate the DB connection")
	}

	return &storeWrapper{
		DB:  db,
		ctx: ctx,
	}
}
