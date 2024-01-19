package format

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
)

func TestGetFormatParser(t *testing.T) {
	validFormats := []string{constants.DBZPostgresAltFormat, constants.DBZPostgresFormat, constants.DBZMongoFormat}
	for _, validFormat := range validFormats {
		assert.NotNil(t, GetFormatParser(validFormat, "topicA"))
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
