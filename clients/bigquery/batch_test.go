package bigquery

import (
	"cloud.google.com/go/bigquery"

	"github.com/stretchr/testify/assert"
)

func (b *BigQueryTestSuite) TestBatch_IsValid() {
	type _testCase struct {
		name        string
		msgs        []*Row
		chunkSize   int
		expectError bool
	}

	testCases := []_testCase{
		{
			name: "happy path",
			msgs: []*Row{
				NewRow(map[string]bigquery.Value{"col1": "message1"}),
				NewRow(map[string]bigquery.Value{"col1": "message2"}),
			},
			chunkSize: 2,
		},
		{
			name: "happy path (chunkSize = 0)",
			msgs: []*Row{
				NewRow(map[string]bigquery.Value{"col1": "message1"}),
				NewRow(map[string]bigquery.Value{"col1": "message2"}),
			},
			expectError: true,
		},
		{
			name: "happy path (chunkSize = -5)",
			msgs: []*Row{
				NewRow(map[string]bigquery.Value{"col1": "message1"}),
				NewRow(map[string]bigquery.Value{"col1": "message2"}),
			},
			chunkSize:   -5,
			expectError: true,
		},
		{
			name:        "batch is empty",
			chunkSize:   2,
			expectError: true,
		},
	}

	for _, testCase := range testCases {
		batch := NewBatch(testCase.msgs, testCase.chunkSize)
		actualErr := batch.IsValid()
		if testCase.expectError {
			assert.Error(b.T(), actualErr, testCase.name)
		} else {
			assert.NoError(b.T(), actualErr, testCase.name)
		}
	}
}

func (b *BigQueryTestSuite) TestBatch_NextChunk() {
	messages := []*Row{
		NewRow(map[string]bigquery.Value{"col1": "message1"}),
		NewRow(map[string]bigquery.Value{"col1": "message2"}),
		NewRow(map[string]bigquery.Value{"col1": "message3"}),
	}

	batch := NewBatch(messages, 2)
	// First call to NextChunk
	chunk := batch.NextChunk()
	assert.Equal(b.T(), 2, len(chunk), "Expected chunk size to be 2")
	assert.Equal(b.T(), map[string]bigquery.Value{"col1": "message1"}, chunk[0].data, "Expected first message in chunk to be message1")
	assert.Equal(b.T(), map[string]bigquery.Value{"col1": "message2"}, chunk[1].data, "Expected second message in chunk to be message2")

	// Second call to NextChunk
	chunk = batch.NextChunk()
	assert.Equal(b.T(), 1, len(chunk), "Expected chunk size to be 1 for the remaining messages")
	assert.Equal(b.T(), map[string]bigquery.Value{"col1": "message3"}, chunk[0].data, "Expected the last message in chunk to be message3")

	// Third call to NextChunk should return an empty chunk as there are no more messages
	chunk = batch.NextChunk()
	assert.Empty(b.T(), chunk, "Expected an empty chunk when there are no more messages")
}
