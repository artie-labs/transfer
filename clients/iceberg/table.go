package iceberg

import (
	"context"
	"encoding/json"

	"github.com/artie-labs/transfer/lib/apachelivy"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s Store) describeTable(ctx context.Context, tableID sql.TableIdentifier) ([]columns.Column, error) {
	query, _, _ := s.Dialect().BuildDescribeTableQuery(tableID)
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

	returnedCols, err := resp.BuildColumns()
	if err != nil {
		return nil, err
	}

	cols := make([]columns.Column, len(returnedCols))
	for i, returnedCol := range returnedCols {
		kind, err := s.Dialect().KindForDataType(returnedCol.DataType, returnedCol.DataType)
		if err != nil {
			return nil, err
		}

		cols[i] = columns.NewColumn(returnedCol.Name, kind)
	}

	return cols, nil
}
