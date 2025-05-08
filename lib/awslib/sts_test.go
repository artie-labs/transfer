package awslib

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCredentials_IsExpired(t *testing.T) {
	creds := Credentials{
		awsAccessKeyID:     "test",
		awsSecretAccessKey: "test",
		awsSessionToken:    "test",
	}

	{
		// Expiration = true, because there's no expiration set
		assert.True(t, creds.isExpired())
	}
	{
		// Expiration = true, because is in the past, so it has expired
		creds.expiresAt = time.Now().Add(-1 * time.Minute)
		assert.True(t, creds.isExpired())
	}
	{
		// Expiration = true, because is in the future (but less than the buffer)
		creds.expiresAt = time.Now().Add(1 * time.Minute)
		assert.True(t, creds.isExpired())
	}
	{
		// Expiration = false, because is in the future (but more than the buffer)
		creds.expiresAt = time.Now().Add(100 * time.Minute)
		assert.False(t, creds.isExpired())
	}
}
