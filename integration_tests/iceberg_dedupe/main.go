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
	totalRows        = 10_000_000
	duplicatePercent = 20
	batchSize        = 100_000
)

type DedupeTest struct {
	store       *iceberg.Store
	tableID     sql.TableIdentifier
	topicConfig kafkalib.TopicConfig
	rng         *rand.Rand
}

func NewDedupeTest(store *iceberg.Store, topicConfig kafkalib.TopicConfig) *DedupeTest {
	return &DedupeTest{
		store:       store,
		tableID:     store.IdentifierFor(topicConfig.BuildDatabaseAndSchemaPair(), topicConfig.TableName),
		topicConfig: topicConfig,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (dt *DedupeTest) createTableData() *optimization.TableData {
	cols := columns.NewColumns(nil)
	cols.AddColumn(columns.NewColumn("id", typing.Integer))
	cols.AddColumn(columns.NewColumn("value", typing.Integer))
	return optimization.NewTableData(cols, config.Replication, []string{"id"}, dt.topicConfig, dt.tableID.Table())
}

func (dt *DedupeTest) insertDataWithDuplicates(ctx context.Context) (uniquePKs int, err error) {
	uniqueCount := totalRows * (100 - duplicatePercent) / 100
	duplicateCount := totalRows - uniqueCount

	slog.Info("Starting insertion", slog.Int("total", totalRows), slog.Int("unique", uniqueCount), slog.Int("duplicates", duplicateCount))

	// Pre-generate all PKs: unique ones followed by duplicates (random picks from unique set)
	allPKs := make([]int, 0, totalRows)
	for i := 0; i < uniqueCount; i++ {
		allPKs = append(allPKs, i)
	}
	for i := 0; i < duplicateCount; i++ {
		allPKs = append(allPKs, dt.rng.Intn(uniqueCount))
	}
	// Shuffle to distribute duplicates randomly
	dt.rng.Shuffle(len(allPKs), func(i, j int) { allPKs[i], allPKs[j] = allPKs[j], allPKs[i] })

	startTime := time.Now()
	for batchStart := 0; batchStart < len(allPKs); batchStart += batchSize {
		batchEnd := min(batchStart+batchSize, len(allPKs))
		tableData := dt.createTableData()

		// Each row in the batch must have a unique map key, so use batch index
		for i, pk := range allPKs[batchStart:batchEnd] {
			key := fmt.Sprintf("%d_%d", batchStart, i)
			tableData.InsertRow(key, map[string]any{"id": pk, "value": pk * 10}, false)
		}

		if err := dt.store.Append(ctx, tableData, nil, false); err != nil {
			return 0, fmt.Errorf("append failed at row %d: %w", batchStart, err)
		}

		if batchEnd%1_000_000 < batchSize {
			slog.Info("Progress", slog.Int("rows", batchEnd), slog.Duration("elapsed", time.Since(startTime)))
		}
	}

	slog.Info("Insertion complete", slog.Int("rows", totalRows), slog.Duration("elapsed", time.Since(startTime)))
	return uniqueCount, nil
}

func (dt *DedupeTest) queryCount(ctx context.Context) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", dt.tableID.FullyQualifiedName())
	resp, err := dt.store.GetApacheLivyClient().QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}

	bytes, err := resp.MarshalJSON()
	if err != nil {
		return 0, err
	}

	var result apachelivy.GetSchemaResponse
	if err := json.Unmarshal(bytes, &result); err != nil {
		return 0, err
	}

	if len(result.Data) == 0 {
		return 0, fmt.Errorf("no data returned")
	}

	count, ok := result.Data[0][0].(float64)
	if !ok {
		return 0, fmt.Errorf("count is not a number")
	}
	return int(count), nil
}

func (dt *DedupeTest) verifyNoDuplicates(ctx context.Context) error {
	query := fmt.Sprintf("SELECT id FROM %s GROUP BY id HAVING COUNT(*) > 1 LIMIT 1", dt.tableID.FullyQualifiedName())
	resp, err := dt.store.GetApacheLivyClient().QueryContext(ctx, query)
	if err != nil {
		return err
	}

	bytes, err := resp.MarshalJSON()
	if err != nil {
		return err
	}

	var result apachelivy.GetSchemaResponse
	if err := json.Unmarshal(bytes, &result); err != nil {
		return err
	}

	if len(result.Data) > 0 {
		return fmt.Errorf("duplicates found: %v", result.Data)
	}
	return nil
}

func (dt *DedupeTest) Run(ctx context.Context) error {
	// Cleanup
	_ = dt.store.DropTable(ctx, dt.tableID.WithTemporaryTable(true))

	// Insert with duplicates
	uniquePKs, err := dt.insertDataWithDuplicates(ctx)
	if err != nil {
		return err
	}

	// Verify initial count
	count, err := dt.queryCount(ctx)
	if err != nil {
		return fmt.Errorf("count query failed: %w", err)
	}
	if count != totalRows {
		return fmt.Errorf("expected %d rows before dedupe, got %d", totalRows, count)
	}
	slog.Info("Verified initial count", slog.Int("count", count))

	// Run dedupe
	slog.Info("Running dedupe...")
	startTime := time.Now()
	if err := dt.store.Dedupe(ctx, dt.tableID, dt.topicConfig.BuildDatabaseAndSchemaPair(), []string{"id"}, false); err != nil {
		return fmt.Errorf("dedupe failed: %w", err)
	}
	slog.Info("Dedupe complete", slog.Duration("elapsed", time.Since(startTime)))

	// Verify final count
	count, err = dt.queryCount(ctx)
	if err != nil {
		return fmt.Errorf("count query failed: %w", err)
	}
	if count != uniquePKs {
		return fmt.Errorf("expected %d rows after dedupe, got %d", uniquePKs, count)
	}
	slog.Info("Verified final count", slog.Int("count", count))

	// Verify no duplicates
	if err := dt.verifyNoDuplicates(ctx); err != nil {
		return fmt.Errorf("duplicate check failed: %w", err)
	}
	slog.Info("Verified no duplicates")

	// Cleanup
	_ = dt.store.DropTable(ctx, dt.tableID.WithTemporaryTable(true))

	return nil
}

func main() {
	ctx := context.Background()
	slog.Info("Starting Iceberg dedupe test", slog.Int("total_rows", totalRows), slog.Int("duplicate_percent", duplicatePercent))

	settings, err := config.LoadSettings(os.Args, true)
	if err != nil {
		logger.Fatal("Failed to load settings", slog.Any("err", err))
	}

	if settings.Config.Output != constants.Iceberg {
		logger.Fatal("This test only supports Iceberg", slog.String("output", string(settings.Config.Output)))
	}

	store, err := iceberg.LoadStore(ctx, settings.Config)
	if err != nil {
		logger.Fatal("Failed to load store", slog.Any("err", err))
	}

	tc := settings.Config.TopicConfigs()
	if len(tc) != 1 {
		logger.Fatal("Expected 1 topic config", slog.Int("count", len(tc)))
	}

	if err := NewDedupeTest(&store, *tc[0]).Run(ctx); err != nil {
		logger.Fatal("Test failed", slog.Any("err", err))
	}

	slog.Info("Test passed")
}
