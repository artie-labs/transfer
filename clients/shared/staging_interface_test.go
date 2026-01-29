package shared

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateReusableStagingTableName(t *testing.T) {
	{
		// No prefix
		result := GenerateReusableStagingTableName("", "users", "abc123")
		assert.Equal(t, "users___artie_abc123", result)
	}
	{
		// With prefix
		result := GenerateReusableStagingTableName("public", "users", "abc123")
		assert.Equal(t, "public__users___artie_abc123", result)
	}
}

func TestGenerateMSMTableName(t *testing.T) {
	{
		// No prefix
		result := GenerateMSMTableName("", "users")
		assert.Equal(t, "__artie_users_msm", result)
	}
	{
		// With prefix
		result := GenerateMSMTableName("public", "users")
		assert.Equal(t, "__artie_public__users_msm", result)
	}
}
