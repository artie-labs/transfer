package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

type Version struct {
	MajorVersion int
	MinorVersion int
}

func RetrieveVersion(ctx context.Context, db *sql.DB) (Version, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return Version{}, fmt.Errorf("failed to scan version: %w", err)
	}

	pgVersion, err := parseVersion(version)
	if err != nil {
		slog.Error("Failed to parse version", slog.String("version", version), slog.Any("err", err))
		return Version{}, fmt.Errorf("failed to parse version: %w", err)
	}

	return Version{
		MajorVersion: pgVersion.MajorVersion,
		MinorVersion: pgVersion.MinorVersion,
	}, nil
}

func parseVersion(versionString string) (Version, error) {
	parts := strings.Split(versionString, " ")
	if len(parts) < 2 {
		return Version{}, fmt.Errorf("invalid version string: %s", versionString)
	}

	if parts[0] != "PostgreSQL" {
		return Version{}, fmt.Errorf("invalid version string: %s", versionString)
	}

	versionParts := strings.Split(parts[1], ".")
	if len(versionParts) < 2 {
		return Version{}, fmt.Errorf("invalid version string: %s", versionString)
	}

	majorVersion, err := strconv.Atoi(versionParts[0])
	if err != nil {
		return Version{}, fmt.Errorf("failed to parse major version: %w", err)
	}

	minorVersion, err := strconv.Atoi(versionParts[1])
	if err != nil {
		return Version{}, fmt.Errorf("failed to parse minor version: %w", err)
	}

	return Version{
		MajorVersion: majorVersion,
		MinorVersion: minorVersion,
	}, nil
}
