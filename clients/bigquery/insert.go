package bigquery

import (
	"fmt"
	"strings"
)

// We will insert 1k rows at a time.
const defaultBatchSize = 1000

type InsertArgs struct {
	BatchSize int
	Rows      []string
	Cols      []string
	TableName string
}

func (s *Store) Insert(insertArgs InsertArgs) error {
	batchSize := defaultBatchSize
	if insertArgs.BatchSize > 0 {
		batchSize = insertArgs.BatchSize
	}

	for i := 0; i < len(insertArgs.Rows); i += batchSize {
		end := i + batchSize
		if end > len(insertArgs.Rows) {
			end = len(insertArgs.Rows)
		}

		fmt.Println("query", fmt.Sprintf("INSERT INTO %s ( %s ) VALUES %s", insertArgs.TableName, strings.Join(insertArgs.Cols, ","), strings.Join(insertArgs.Rows[i:end], ",")))
		_, err := s.Exec(fmt.Sprintf("INSERT INTO %s ( %s ) VALUES %s", insertArgs.TableName, strings.Join(insertArgs.Cols, ","), strings.Join(insertArgs.Rows[i:end], ",")))
		if err != nil {
			return err
		}
	}

	return nil
}
