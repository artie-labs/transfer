package iceberg

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s Store) DescribeTable(ctx context.Context, tableID sql.TableIdentifier) ([]columns.Column, error) {
	query, _, _ := s.dialect().BuildDescribeTableQuery(tableID)
	response, err := s.apacheLivyClient.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	out, ok := response.Output.Data["application/json"]
	if !ok {
		return nil, fmt.Errorf("unexpected data format")
	}

	fmt.Println("out", out)
	return nil, nil
}
