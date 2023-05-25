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

	if driverName == "snowflake" {
		// This is required because Golang's SQL driver will spin up additional connections.
		// However, Snowflake temporary tables only last for the duration of the session.
		// This means, at scale - we can run into a scenario where one Go-routine has been allocated a new connection
		// Which does not have visibility into the previously created temporary table.
		// https://github.com/snowflakedb/gosnowflake/issues/181#issuecomment-411904223
		db.SetMaxIdleConns(1)
		db.SetMaxOpenConns(1)
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
