package db

import (
	"context"
	"database/sql"

	"github.com/artie-labs/transfer/lib/logger"
)

type Store interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
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

	return db
}
