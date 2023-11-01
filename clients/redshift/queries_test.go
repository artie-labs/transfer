package redshift

import "github.com/stretchr/testify/assert"

func (r *RedshiftTestSuite) Test_DescribeTableQuery() {
	type _tc struct {
		tableName string
		expectErr bool
	}

	tcs := []_tc{
		{
			tableName: "delta",
		},
		{
			tableName: "delta123",
		},
		{
			tableName: `"delta"`,
			expectErr: true,
		},
	}

	for _, tc := range tcs {
		_, err := describeTableQuery(describeArgs{
			RawTableName: tc.tableName,
			Schema:       "foo",
		})

		if tc.expectErr {
			assert.Error(r.T(), err, tc.tableName)
		} else {
			assert.NoError(r.T(), err, tc.tableName)
		}
	}
}
