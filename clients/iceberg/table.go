package iceberg

import (
	"context"
	"encoding/json"

	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s Store) describeTable(ctx context.Context, tableID sql.TableIdentifier) ([]columns.Column, error) {
	query, _, _ := s.dialect().BuildDescribeTableQuery(tableID)
	output, err := s.apacheLivyClient.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	bytes, err := output.MarshalJSON()
	if err != nil {
		return nil, err
	}

	var resp apachelivy.GetSchemaResponse
	if err := json.Unmarshal(bytes, &resp); err != nil {
		return nil, err
	}

	_cols, err := resp.BuildColumns()
	if err != nil {
		return nil, err
	}

	cols := make([]columns.Column, len(_cols))
	for i, col := range _cols {
		kind, err := s.dialect().KindForDataType(col.DataType, col.DataType)
		if err != nil {
			return nil, err
		}

		cols[i] = columns.NewColumn(col.Name, kind)
	}

	return cols, nil
}
