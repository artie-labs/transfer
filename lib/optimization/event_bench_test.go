package optimization

import (
	"fmt"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

func BenchmarkTableData_ApproxSize_TallTable(b *testing.B) {
	td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "tallTable")
	for n := 0; n < b.N; n++ {
		td.InsertRow(fmt.Sprint(n), map[string]any{
			"id":   n,
			"name": "Robin",
			"dog":  "dusty the mini aussie",
		}, constants.Create)
	}
}

func BenchmarkTableData_ApproxSize_WideTable(b *testing.B) {
	td := NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "wideTable")
	for n := 0; n < b.N; n++ {
		td.InsertRow(fmt.Sprint(n), map[string]any{
			"id":                 n,
			"name":               "Robin",
			"dog":                "dusty the mini aussie",
			"favorite_fruits":    []string{"strawberry", "kiwi", "oranges"},
			"random":             false,
			"team":               []string{"charlie", "jacqueline"},
			"email":              "robin@example.com",
			"favorite_languages": []string{"go", "sql"},
			"favorite_databases": []string{"postgres", "bigtable"},
			"created_at":         time.Now(),
			"updated_at":         time.Now(),
			"negative_number":    -500,
			"nestedObject": map[string]any{
				"foo": "bar",
				"abc": "def",
			},
			"array_of_objects": []map[string]any{
				{
					"foo": "bar",
				},
				{
					"foo_nested": map[string]any{
						"foo_foo": "bar_bar",
					},
				},
			},
			"is_deleted":   false,
			"lorem_ipsum":  "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec elementum aliquet mi at efficitur. Praesent at erat ac elit faucibus convallis. Donec fermentum tellus eu nunc ornare, non convallis justo facilisis. In hac habitasse platea dictumst. Praesent eu ante vitae erat semper finibus eget ac mauris. Duis gravida cursus enim, nec sagittis arcu placerat sed. Integer semper orci justo, nec rhoncus libero convallis sed.",
			"lorem_ipsum2": "Fusce vitae elementum tortor. Vestibulum consectetur ante id nibh ullamcorper, quis sodales turpis tempor. Duis pellentesque suscipit nibh porta posuere. In libero massa, efficitur at ultricies sit amet, vulputate ac ante. In euismod erat eget nulla blandit pretium. Ut tempor ante vel congue venenatis. Vestibulum at metus nec nibh iaculis consequat suscipit ac leo. Maecenas vitae rutrum nulla, quis ultrices justo. Aliquam ipsum ex, luctus ac diam eget, tempor tempor risus.",
		}, constants.Create)
	}
}
