package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

func RetrieveVersion(ctx context.Context, db *sql.DB) (int, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return 0, fmt.Errorf("failed to scan version: %w", err)
	}

	pgVersion, err := parseVersion(version)
	if err != nil {
		slog.Error("Failed to parse version", slog.String("version", version), slog.Any("err", err))
		return 0, fmt.Errorf("failed to parse version: %w", err)
	}

	return pgVersion, nil
}

func parseVersion(versionString string) (int, error) {
	parts := strings.Split(versionString, " ")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid version string: %s", versionString)
	}

	if parts[0] != "PostgreSQL" {
		return 0, fmt.Errorf("invalid version string: %s", versionString)
	}

	versionParts := strings.Split(parts[1], ".")
	if len(versionParts) < 2 {
		return 0, fmt.Errorf("invalid version string: %s", versionString)
	}

	return strconv.Atoi(versionParts[0])
}
