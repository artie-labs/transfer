package format

import (
	"context"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
	"os"
	"os/exec"
	"testing"
)

func TestGetFormatParser(t *testing.T) {
	ctx := context.Background()
	validFormats := []string{constants.DBZPostgresAltFormat, constants.DBZPostgresFormat, constants.DBZMongoFormat}
	for _, validFormat := range validFormats {
		assert.NotNil(t, GetFormatParser(ctx, validFormat))
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
		GetFormatParser(context.Background(), "foo")
	})
}
