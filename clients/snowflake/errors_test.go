package snowflake

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTableDoesNotExistErr(t *testing.T) {
	errToExpectation := map[error]bool{
		nil: false,
		fmt.Errorf("Table 'DATABASE.SCHEMA.TABLE' does not exist or not authorized"): true,
		fmt.Errorf("hi this is super random"):                                        false,
	}

	for err, expectation := range errToExpectation {
		assert.Equal(t, TableDoesNotExistErr(err), expectation, err)
	}
}
