package snowflake

import (
	"log/slog"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) Append(tableData *optimization.TableData) error {
	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())

	var err error
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			if IsAuthExpiredError(err) {
				slog.Warn("Authentication has expired, will reload the Snowflake store and retry appending", slog.Any("err", err))
				if connErr := s.reestablishConnection(); connErr != nil {
					// TODO: Remove this panic and return an error instead. Ensure the callers of [Append] handle this properly.
					logger.Panic("Failed to reestablish connection", slog.Any("err", connErr))
				}
			} else {
				break
			}
		}

		temporaryTableID := shared.TempTableID(tableID, tableData.TempTableSuffix())
		err = shared.Append(s, tableData, types.AppendOpts{
			TempTableID:          temporaryTableID,
			AdditionalCopyClause: `FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE) PURGE = TRUE`,
		})

		if err == nil {
			_, err = s.Exec(`INSERT INTO %s SELECT * FROM %s`, tableID.FullyQualifiedName(), temporaryTableID.FullyQualifiedName())
		}
	}

	return err
}

func (s *Store) Merge(tableData *optimization.TableData) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			if IsAuthExpiredError(err) {
				slog.Warn("Authentication has expired, will reload the Snowflake store and retry merging", slog.Any("err", err))
				if connErr := s.reestablishConnection(); connErr != nil {
					// TODO: Remove this panic and return an error instead. Ensure the callers of [Merge] handle this properly.
					logger.Panic("Failed to reestablish connection", slog.Any("err", connErr))
				}
			} else {
				break
			}
		}

		err = shared.Merge(s, tableData, s.config, types.MergeOpts{})
	}
	return err
}
