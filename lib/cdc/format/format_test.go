package format

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/cdc/relational"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
)

func TestGetFormatParser(t *testing.T) {
	{
		// Relational
		for _, format := range []string{constants.DBZPostgresAltFormat, constants.DBZPostgresFormat} {
			formatParser := GetFormatParser(format, "topicA")
			assert.NotNil(t, formatParser)

			_, err := typing.AssertType[relational.Debezium](formatParser)
			assert.NoError(t, err)
		}
	}
	{
		// Mongo
		formatParser := GetFormatParser(constants.DBZMongoFormat, "topicA")
		assert.NotNil(t, formatParser)

		_, err := typing.AssertType[mongo.Debezium](formatParser)
		assert.NoError(t, err)
	}
}

func testOsExit(t *testing.T, testFunc func(*testing.T)) {
	if os.Getenv(t.Name()) == "1" {
		testFunc(t)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run="+t.Name())
	cmd.Env = append(os.Environ(), t.Name()+"=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}

	t.Fatal("subprocess ran successfully, want non-zero exit status")
}

func TestGetFormatParserFatal(t *testing.T) {
	// This test cannot be iterated because it forks a separate process to do `go test -test.run=...`
	testOsExit(t, func(t *testing.T) {
		GetFormatParser("foo", "topicB")
	})
}
