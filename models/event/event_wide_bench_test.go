package event

import (
	"context"
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/models"
)

// BenchmarkSave_WideTable benchmarks Save() with 148 columns to identify bottlenecks
func BenchmarkSave_WideTable(b *testing.B) {
	const numColumns = 148

	// Create a wide row with 148 columns
	data := make(map[string]any, numColumns+3)
	data["id"] = "pk-123"
	data[constants.DeleteColumnMarker] = false
	data[constants.OnlySetDeleteColumnMarker] = false
	for i := 0; i < numColumns; i++ {
		data[fmt.Sprintf("col_%d", i)] = fmt.Sprintf("value_%d", i)
	}

	tc := kafkalib.TopicConfig{
		Database:  "test_db",
		TableName: "wide_table",
		Schema:    "public",
	}

	cfg := config.Config{
		Mode:       config.Replication,
		BufferRows: 10000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := models.NewMemoryDB()
		evt := Event{
			table:       "wide_table",
			tableID:     cdc.NewTableID("public", "wide_table"),
			data:        copyMap(data),
			primaryKeys: []string{"id"},
			mode:        config.Replication,
		}
		b.StartTimer()

		_, _, err := evt.Save(cfg, db, tc, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSave_WideTable_WithColumns benchmarks Save() when columns are pre-populated
func BenchmarkSave_WideTable_WithColumns(b *testing.B) {
	const numColumns = 148

	// Create columns
	cols := &columns.Columns{}
	for i := 0; i < numColumns; i++ {
		cols.AddColumn(columns.NewColumn(fmt.Sprintf("col_%d", i), typing.String))
	}
	cols.AddColumn(columns.NewColumn("id", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))
	cols.AddColumn(columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean))

	// Create a wide row with 148 columns
	data := make(map[string]any, numColumns+3)
	data["id"] = "pk-123"
	data[constants.DeleteColumnMarker] = false
	data[constants.OnlySetDeleteColumnMarker] = false
	for i := 0; i < numColumns; i++ {
		data[fmt.Sprintf("col_%d", i)] = fmt.Sprintf("value_%d", i)
	}

	tc := kafkalib.TopicConfig{
		Database:  "test_db",
		TableName: "wide_table",
		Schema:    "public",
	}

	cfg := config.Config{
		Mode:       config.Replication,
		BufferRows: 10000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := models.NewMemoryDB()
		evt := Event{
			table:       "wide_table",
			tableID:     cdc.NewTableID("public", "wide_table"),
			data:        copyMap(data),
			columns:     cols,
			primaryKeys: []string{"id"},
			mode:        config.Replication,
		}
		b.StartTimer()

		_, _, err := evt.Save(cfg, db, tc, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkColumns_GetColumn benchmarks the O(n) GetColumn operation
func BenchmarkColumns_GetColumn(b *testing.B) {
	const numColumns = 148

	cols := &columns.Columns{}
	for i := 0; i < numColumns; i++ {
		cols.AddColumn(columns.NewColumn(fmt.Sprintf("col_%d", i), typing.String))
	}

	// Lookup the last column (worst case)
	targetCol := fmt.Sprintf("col_%d", numColumns-1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cols.GetColumn(targetCol)
	}
}

// BenchmarkToMemoryEvent_WideTable benchmarks ToMemoryEvent with 148 columns
func BenchmarkToMemoryEvent_WideTable(b *testing.B) {
	const numColumns = 148

	// Create mock event data
	data := make(map[string]any, numColumns+3)
	data["id"] = "pk-123"
	data[constants.DeleteColumnMarker] = false
	data[constants.OnlySetDeleteColumnMarker] = false
	for i := 0; i < numColumns; i++ {
		data[fmt.Sprintf("col_%d", i)] = fmt.Sprintf("value_%d", i)
	}

	mockEvent := &mocks.FakeEvent{}
	mockEvent.GetTableNameReturns("wide_table")
	mockEvent.GetDataReturns(data, nil)
	mockEvent.OperationReturns(constants.Create)

	tc := kafkalib.TopicConfig{
		Database:  "test_db",
		TableName: "wide_table",
		Schema:    "public",
	}

	pkMap := map[string]any{"id": "pk-123"}
	fakeBaseline := &mocks.FakeBaseline{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ToMemoryEvent(context.Background(), fakeBaseline, mockEvent, pkMap, tc, config.Replication)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func copyMap(m map[string]any) map[string]any {
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// BenchmarkSave_WideTable_SubsequentRows simulates processing multiple rows into the same table
// This is the realistic scenario - columns are already in memory
func BenchmarkSave_WideTable_SubsequentRows(b *testing.B) {
	const numColumns = 148

	// Create columns (simulating they're already in the destination)
	cols := &columns.Columns{}
	for i := 0; i < numColumns; i++ {
		cols.AddColumn(columns.NewColumn(fmt.Sprintf("col_%d", i), typing.String))
	}
	cols.AddColumn(columns.NewColumn("id", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))
	cols.AddColumn(columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean))

	tc := kafkalib.TopicConfig{
		Database:  "test_db",
		TableName: "wide_table",
		Schema:    "public",
	}

	cfg := config.Config{
		Mode:       config.Replication,
		BufferRows: 100000,
	}

	// Pre-populate the in-memory database with one row to initialize the table
	db := models.NewMemoryDB()
	initialData := make(map[string]any, numColumns+3)
	initialData["id"] = "pk-0"
	initialData[constants.DeleteColumnMarker] = false
	initialData[constants.OnlySetDeleteColumnMarker] = false
	for i := 0; i < numColumns; i++ {
		initialData[fmt.Sprintf("col_%d", i)] = fmt.Sprintf("value_%d", i)
	}

	initialEvt := Event{
		table:       "wide_table",
		tableID:     cdc.NewTableID("public", "wide_table"),
		data:        initialData,
		columns:     cols,
		primaryKeys: []string{"id"},
		mode:        config.Replication,
	}
	_, _, _ = initialEvt.Save(cfg, db, tc, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := make(map[string]any, numColumns+3)
		data["id"] = fmt.Sprintf("pk-%d", i+1)
		data[constants.DeleteColumnMarker] = false
		data[constants.OnlySetDeleteColumnMarker] = false
		for j := 0; j < numColumns; j++ {
			data[fmt.Sprintf("col_%d", j)] = fmt.Sprintf("value_%d_%d", i, j)
		}

		evt := Event{
			table:       "wide_table",
			tableID:     cdc.NewTableID("public", "wide_table"),
			data:        data,
			columns:     cols,
			primaryKeys: []string{"id"},
			mode:        config.Replication,
		}

		_, _, err := evt.Save(cfg, db, tc, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadOnlyInMemoryCols benchmarks the column copy operation
func BenchmarkReadOnlyInMemoryCols(b *testing.B) {
	const numColumns = 148

	cols := &columns.Columns{}
	for i := 0; i < numColumns; i++ {
		cols.AddColumn(columns.NewColumn(fmt.Sprintf("col_%d", i), typing.String))
	}

	tc := kafkalib.TopicConfig{
		Database:  "test_db",
		TableName: "wide_table",
		Schema:    "public",
	}

	cfg := config.Config{
		Mode:       config.Replication,
		BufferRows: 100000,
	}

	db := models.NewMemoryDB()
	tableID := cdc.NewTableID("public", "wide_table")
	td := db.GetOrCreateTableData(tableID, "topic")
	td.SetTableData(optimization.NewTableData(cols, cfg.Mode, []string{"id"}, tc, "wide_table"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = td.ReadOnlyInMemoryCols()
	}
}

