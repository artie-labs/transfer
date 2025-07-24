package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type pgxStoreWrapper struct {
	*pgx.Conn
}

func (p *pgxStoreWrapper) IsRetryableError(err error) bool {
	return isRetryableError(err)
}

func OpenPGX(ctx context.Context, dsn string) (Store, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the database: %w", err)
	}

	return &pgxStoreWrapper{conn}, nil
}
