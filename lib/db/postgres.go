package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

type VersionResult struct {
	Major int
	Minor int
}

func RetrieveVersion(ctx context.Context, db *sql.DB) (VersionResult, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return VersionResult{}, fmt.Errorf("failed to scan version: %w", err)
	}

	result, err := parseVersion(version)
	if err != nil {
		slog.Error("Failed to parse version", slog.String("version", version), slog.Any("err", err))
		return VersionResult{}, fmt.Errorf("failed to parse version: %w", err)
	}

	return VersionResult{
		Major: result.Major,
		Minor: result.Minor,
	}, nil
}

func parseVersion(versionString string) (VersionResult, error) {
	parts := strings.Split(versionString, " ")
	if len(parts) < 2 {
		return VersionResult{}, fmt.Errorf("invalid version string: %s", versionString)
	}

	if parts[0] != "PostgreSQL" {
		return VersionResult{}, fmt.Errorf("invalid version string: %s", versionString)
	}

	versionParts := strings.Split(parts[1], ".")
	if len(versionParts) < 2 {
		return VersionResult{}, fmt.Errorf("invalid version string: %s", versionString)
	}

	majorVersion, err := strconv.Atoi(versionParts[0])
	if err != nil {
		return VersionResult{}, fmt.Errorf("failed to parse major version: %w", err)
	}

	minorVersion, err := strconv.Atoi(versionParts[1])
	if err != nil {
		return VersionResult{}, fmt.Errorf("failed to parse minor version: %w", err)
	}

	return VersionResult{
		Major: majorVersion,
		Minor: minorVersion,
	}, nil
}
