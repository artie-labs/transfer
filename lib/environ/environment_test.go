package environ

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMustGetEnv(t *testing.T) {
	{
		// Single environment variable is set
		t.Setenv("TEST_ENV_VAR", "test")
		assert.NoError(t, MustGetEnv("TEST_ENV_VAR"))
	}
	{
		// Multiple environment variables are set
		t.Setenv("TEST_ENV_VAR_2", "test2")
		t.Setenv("TEST_ENV_VAR_3", "test3")
		assert.NoError(t, MustGetEnv("TEST_ENV_VAR", "TEST_ENV_VAR_2", "TEST_ENV_VAR_3"))
	}
	{
		// Environment variable is not set
		assert.ErrorContains(t, MustGetEnv("NONEXISTENT_ENV_VAR"), `required environment variables "NONEXISTENT_ENV_VAR" are not set`)
	}
	{
		// Multiple environment variables, some not set
		t.Setenv("TEST_ENV_VAR_4", "test4")
		assert.ErrorContains(t, MustGetEnv("TEST_ENV_VAR_4", "NONEXISTENT_ENV_VAR_2", "NONEXISTENT_ENV_VAR_3"), `required environment variables "NONEXISTENT_ENV_VAR_2, NONEXISTENT_ENV_VAR_3" are not set`)
	}
}
