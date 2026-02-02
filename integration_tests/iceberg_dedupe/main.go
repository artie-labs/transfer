package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/artie-labs/transfer/clients/iceberg"
	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

const (
	// Configuration for the dedupe test
	totalRows        = 10_000_000 // 10M total rows to insert
	duplicatePercent = 20         // 20% of rows will be duplicates
	batchSize        = 100_000    // Insert in batches of 100k
)

type DedupeTest struct {
	store       *iceberg.Store
	tableID     sql.TableIdentifier
	topicConfig kafkalib.TopicConfig
	rng         *rand.Rand
}

func NewDedupeTest(store *iceberg.Store, topicConfig kafkalib.TopicConfig) *DedupeTest {
	tableID := store.IdentifierFor(topicConfig.BuildDatabaseAndSchemaPair(), topicConfig.TableName)
	return &DedupeTest{
		store:       store,
		tableID:     tableID,
		topicConfig: topicConfig,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (dt *DedupeTest) setupColumns() *columns.Columns {
	cols := columns.NewColumns(nil)
	colTypes := map[string]typing.KindDetails{
		"id":         typing.Integer,
		"name":       typing.String,
		"created_at": typing.TimestampTZ,
		"value":      typing.Float,
		"data":       typing.String,
	}

	for colName, colType := range colTypes {
		cols.AddColumn(columns.NewColumn(colName, colType))
	}

	return cols
}

func (dt *DedupeTest) generateRowData(pkValue int) map[string]any {
	return map[string]any{
		"id":         pkValue,
		"name":       fmt.Sprintf("test_name_%d", pkValue),
		"created_at": time.Now().Format(time.RFC3339Nano),
		"value":      float64(pkValue) * 1.5,
		"data":       fmt.Sprintf("some_data_for_row_%d", pkValue),
	}
}

func (dt *DedupeTest) createTableData() *optimization.TableData {
	cols := dt.setupColumns()
	return optimization.NewTableData(cols, config.Replication, []string{"id"}, dt.topicConfig, dt.tableID.Table())
}

func (dt *DedupeTest) cleanup(ctx context.Context) error {
	dropTableID := dt.tableID.WithTemporaryTable(true)
	return dt.store.DropTable(ctx, dropTableID)
}

func (dt *DedupeTest) insertDataWithDuplicates(ctx context.Context) (int, int, error) {
	uniqueRows := totalRows * (100 - duplicatePercent) / 100
	duplicateRows := totalRows - uniqueRows

	slog.Info("Starting data insertion",
		slog.Int("total_rows", totalRows),
		slog.Int("unique_rows", uniqueRows),
		slog.Int("duplicate_rows", duplicateRows),
		slog.Int("batch_size", batchSize),
	)

	// Track all PKs from previous batches for creating duplicates
	// InsertRow uses PK as map key, so we must ensure each batch has unique PKs internally
	allPreviousPKs := make([]int, 0, uniqueRows)
	totalRowsInserted := 0
	totalDuplicatesInserted := 0
	nextUniquePK := 0

	startTime := time.Now()

	// Insert in batches
	for totalRowsInserted < totalRows {
		tableData := dt.createTableData()
		pksInCurrentBatch := make(map[int]bool)
		batchDuplicates := 0

		// Fill the batch with exactly batchSize unique rows
		for len(pksInCurrentBatch) < batchSize && totalRowsInserted+len(pksInCurrentBatch) < totalRows {
			var pkValue int
			isDuplicate := false

			// Decide if this row should be a duplicate (only from PREVIOUS batches)
			remainingDuplicates := duplicateRows - totalDuplicatesInserted - batchDuplicates
			remainingRows := totalRows - totalRowsInserted - len(pksInCurrentBatch)
			shouldDuplicate := len(allPreviousPKs) > 0 && remainingDuplicates > 0 && dt.rng.Float64() < float64(remainingDuplicates)/float64(remainingRows)

			if shouldDuplicate {
				// Pick a random PK from PREVIOUS batches to duplicate
				// Keep trying until we find one not already in current batch
				for attempts := 0; attempts < 10; attempts++ {
					candidate := allPreviousPKs[dt.rng.Intn(len(allPreviousPKs))]
					if !pksInCurrentBatch[candidate] {
						pkValue = candidate
						isDuplicate = true
						break
					}
				}
				// If we couldn't find a unique duplicate, create a new row instead
				if !isDuplicate {
					pkValue = nextUniquePK
					nextUniquePK++
				}
			} else {
				// Create a new unique row
				pkValue = nextUniquePK
				nextUniquePK++
			}

			// Skip if this PK is already in the current batch
			if pksInCurrentBatch[pkValue] {
				continue
			}

			pksInCurrentBatch[pkValue] = true
			if isDuplicate {
				batchDuplicates++
			}

			rowData := dt.generateRowData(pkValue)
			pkValueString := fmt.Sprintf("%d", pkValue)
			tableData.InsertRow(pkValueString, rowData, false)
		}

		batchRowCount := len(pksInCurrentBatch)
		totalRowsInserted += batchRowCount
		totalDuplicatesInserted += batchDuplicates

		// Log progress every 1M rows
		if totalRowsInserted%1_000_000 < batchSize {
			elapsed := time.Since(startTime)
			rate := float64(totalRowsInserted) / elapsed.Seconds()
			slog.Info("Insert progress",
				slog.Int("inserted", totalRowsInserted),
				slog.Int("duplicates_so_far", totalDuplicatesInserted),
				slog.Int("unique_pks", nextUniquePK),
				slog.Int("batch_rows", batchRowCount),
				slog.Int("batch_duplicates", batchDuplicates),
				slog.Duration("elapsed", elapsed),
				slog.Float64("rows_per_sec", rate),
			)
		}

		// Add all PKs from this batch to the pool for future duplicates
		// (both new and duplicate PKs can be duplicated again in future batches)
		for pk := range pksInCurrentBatch {
			allPreviousPKs = append(allPreviousPKs, pk)
		}

		// Append this batch to the table
		if err := dt.store.Append(ctx, tableData, nil, false); err != nil {
			return 0, 0, fmt.Errorf("failed to append batch at row %d: %w", totalRowsInserted, err)
		}
	}

	elapsed := time.Since(startTime)
	slog.Info("Data insertion complete",
		slog.Int("total_rows_in_db", totalRowsInserted),
		slog.Int("duplicates_inserted", totalDuplicatesInserted),
		slog.Int("unique_pks", nextUniquePK),
		slog.Int("expected_after_dedupe", nextUniquePK),
		slog.Duration("elapsed", elapsed),
		slog.Float64("rows_per_sec", float64(totalRowsInserted)/elapsed.Seconds()),
	)

	return nextUniquePK, totalDuplicatesInserted, nil
}

func (dt *DedupeTest) verifyRowCount(ctx context.Context, expected int) error {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", dt.tableID.FullyQualifiedName())
	resp, err := dt.store.GetApacheLivyClient().QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query row count: %w", err)
	}

	bytes, err := resp.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	var schemaResp apachelivy.GetSchemaResponse
	if err := json.Unmarshal(bytes, &schemaResp); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(schemaResp.Data) == 0 {
		return fmt.Errorf("no data returned from count query")
	}

	count, ok := schemaResp.Data[0][0].(float64)
	if !ok {
		return fmt.Errorf("row count is not a float")
	}

	if int(count) != expected {
		return fmt.Errorf("unexpected row count: expected %d, got %d", expected, int(count))
	}

	slog.Info("Row count verified", slog.Int("count", int(count)), slog.Int("expected", expected))
	return nil
}

func (dt *DedupeTest) runDedupe(ctx context.Context) error {
	slog.Info("Starting deduplication...")
	startTime := time.Now()

	pair := dt.topicConfig.BuildDatabaseAndSchemaPair()
	primaryKeys := []string{"id"}

	if err := dt.store.Dedupe(ctx, dt.tableID, pair, primaryKeys, false); err != nil {
		return fmt.Errorf("dedupe failed: %w", err)
	}

	elapsed := time.Since(startTime)
	slog.Info("Deduplication complete", slog.Duration("elapsed", elapsed))
	return nil
}

func (dt *DedupeTest) verifyNoDuplicates(ctx context.Context) error {
	// Check that there are no duplicate PKs
	query := fmt.Sprintf(`
		SELECT id, COUNT(*) as cnt 
		FROM %s 
		GROUP BY id 
		HAVING COUNT(*) > 1 
		LIMIT 10
	`, dt.tableID.FullyQualifiedName())

	resp, err := dt.store.GetApacheLivyClient().QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to check for duplicates: %w", err)
	}

	bytes, err := resp.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	var schemaResp apachelivy.GetSchemaResponse
	if err := json.Unmarshal(bytes, &schemaResp); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(schemaResp.Data) > 0 {
		return fmt.Errorf("found %d duplicate PKs after dedupe, first few: %v", len(schemaResp.Data), schemaResp.Data)
	}

	slog.Info("Verified no duplicates remain")
	return nil
}

func (dt *DedupeTest) Run(ctx context.Context) error {
	// Step 1: Cleanup any existing table
	slog.Info("Step 1: Cleaning up existing table...")
	if err := dt.cleanup(ctx); err != nil {
		slog.Warn("Cleanup failed (table may not exist)", slog.Any("err", err))
	}

	// Step 2: Insert data with duplicates
	slog.Info("Step 2: Inserting data with duplicates...")
	uniquePKs, duplicatesInserted, err := dt.insertDataWithDuplicates(ctx)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	// Step 3: Verify initial row count (should be totalRows including duplicates)
	slog.Info("Step 3: Verifying initial row count...")
	if err := dt.verifyRowCount(ctx, totalRows); err != nil {
		return fmt.Errorf("failed to verify initial row count: %w", err)
	}

	// Step 4: Run dedupe
	slog.Info("Step 4: Running dedupe...")
	if err := dt.runDedupe(ctx); err != nil {
		return fmt.Errorf("failed to run dedupe: %w", err)
	}

	// Step 5: Verify row count after dedupe (should be uniquePKs)
	slog.Info("Step 5: Verifying row count after dedupe...")
	if err := dt.verifyRowCount(ctx, uniquePKs); err != nil {
		return fmt.Errorf("failed to verify row count after dedupe: %w", err)
	}

	// Step 6: Verify no duplicates remain
	slog.Info("Step 6: Verifying no duplicates remain...")
	if err := dt.verifyNoDuplicates(ctx); err != nil {
		return fmt.Errorf("failed to verify no duplicates: %w", err)
	}

	// Step 7: Cleanup
	slog.Info("Step 7: Cleaning up...")
	if err := dt.cleanup(ctx); err != nil {
		return fmt.Errorf("failed to cleanup: %w", err)
	}

	slog.Info("Test summary",
		slog.Int("total_rows_inserted", totalRows),
		slog.Int("duplicates_inserted", duplicatesInserted),
		slog.Int("unique_pks", uniquePKs),
		slog.Int("expected_after_dedupe", uniquePKs),
	)

	return nil
}

func main() {
	ctx := context.Background()

	slog.Info("Starting Iceberg dedupe integration test",
		slog.Int("total_rows", totalRows),
		slog.Int("duplicate_percent", duplicatePercent),
		slog.Int("batch_size", batchSize),
	)

	settings, err := config.LoadSettings(os.Args, true)
	if err != nil {
		logger.Fatal("Failed to load settings", slog.Any("err", err))
	}

	if settings.Config.Output != constants.Iceberg {
		logger.Fatal("This test only supports Iceberg destination", slog.String("output", string(settings.Config.Output)))
	}

	store, err := iceberg.LoadStore(ctx, settings.Config)
	if err != nil {
		logger.Fatal("Failed to load Iceberg store", slog.Any("err", err))
	}

	tc := settings.Config.TopicConfigs()
	if len(tc) != 1 {
		logger.Fatal("Expected 1 topic config", slog.Int("num_configs", len(tc)))
	}

	test := NewDedupeTest(&store, *tc[0])
	if err := test.Run(ctx); err != nil {
		logger.Fatal("Test failed", slog.Any("err", err))
	}

	slog.Info("Iceberg dedupe integration test completed successfully")
}
