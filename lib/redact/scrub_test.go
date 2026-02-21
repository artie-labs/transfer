package redact

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScrubString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "normal error message",
			input:    "failed to connect: connection refused",
			expected: "failed to connect: connection refused",
		},
		{
			name:     "postgres connection string",
			input:    `failed to connect to postgres://admin:s3cret@db.example.com:5432/mydb`,
			expected: `failed to connect to postgres://admin:[REDACTED]@db.example.com:5432/mydb`,
		},
		{
			name:     "mongodb connection string",
			input:    `error connecting to mongodb://user:p%40ssw0rd@cluster.mongodb.net:27017/admin`,
			expected: `error connecting to mongodb://user:[REDACTED]@cluster.mongodb.net:27017/admin`,
		},
		{
			name:     "URL without password is unchanged",
			input:    "failed to reach https://api.example.com/v1/data",
			expected: "failed to reach https://api.example.com/v1/data",
		},
		{
			name:     "key=value password",
			input:    "config error: password=hunter2 is invalid",
			expected: "config error: password=[REDACTED] is invalid",
		},
		{
			name:     "key: value secret",
			input:    `unable to authenticate, secret: my-secret-value`,
			expected: `unable to authenticate, secret: [REDACTED]`,
		},
		{
			name:     "JSON key-value api_key",
			input:    `{"api_key": "abc123def456"}`,
			expected: `{"api_key": "[REDACTED]"}`,
		},
		{
			name:     "JSON key-value token",
			input:    `"token": "eyJhbGciOiJIUzI1NiJ9.payload.signature"`,
			expected: `"token": "[REDACTED]"`,
		},
		{
			name:     "access_key in config",
			input:    `access_key=AKIAIOSFODNN7EXAMPLE`,
			expected: `access_key=[REDACTED]`,
		},
		{
			name:     "AWS access key ID",
			input:    "credentials: AKIAIOSFODNN7EXAMPLE are expired",
			expected: "credentials: [REDACTED] are expired",
		},
		{
			name:     "Bearer token",
			input:    "Authorization failed: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig",
			expected: "Authorization failed: Bearer [REDACTED]",
		},
		{
			name:     "Bearer token with base64 padding",
			input:    "Authorization failed: Bearer abc123==",
			expected: "Authorization failed: Bearer [REDACTED]",
		},
		{
			name:     "private key block",
			input:    "error parsing key: -----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBg...\n-----END PRIVATE KEY-----",
			expected: "error parsing key: [REDACTED]",
		},
		{
			name:     "RSA private key block",
			input:    "invalid key:\n-----BEGIN RSA PRIVATE KEY-----\nMIIBog...\n-----END RSA PRIVATE KEY-----\nfailed",
			expected: "invalid key:\n[REDACTED]\nfailed",
		},
		{
			name:     "email address",
			input:    "row failed for user john.doe@example.com in table users",
			expected: "row failed for user [REDACTED] in table users",
		},
		{
			name:     "SSN with dashes",
			input:    "invalid value 123-45-6789 in column ssn",
			expected: "invalid value [REDACTED] in column ssn",
		},
		{
			name:     "SSN with spaces",
			input:    "found SSN: 123 45 6789",
			expected: "found SSN: [REDACTED]",
		},
		{
			name:     "credit card number with spaces",
			input:    "payment failed for card 4111 1111 1111 1111",
			expected: "payment failed for card [REDACTED]",
		},
		{
			name:     "credit card number with dashes",
			input:    "invalid card: 4111-1111-1111-1111",
			expected: "invalid card: [REDACTED]",
		},
		{
			name:     "credit card number no separators",
			input:    "card number 4111111111111111 is invalid",
			expected: "card number [REDACTED] is invalid",
		},
		{
			name:     "multiple patterns in one message",
			input:    "user john@example.com failed to connect to postgres://admin:secret@host/db with SSN 111-22-3333",
			expected: "user [REDACTED] failed to connect to postgres://admin:[REDACTED]@host/db with SSN [REDACTED]",
		},
		{
			name:     "password with special chars in key=value",
			input:    `connection failed: password=p@ss!word&foo=bar`,
			expected: `connection failed: password=[REDACTED]&foo=bar`,
		},
		{
			name:     "credential key variant",
			input:    "credential=super_secret_123 expired",
			expected: "credential=[REDACTED] expired",
		},
		{
			name:     "private_key key-value",
			input:    `"private_key": "-----BEGIN RSA PRIVATE KEY-----\ndata\n-----END RSA PRIVATE KEY-----"`,
			expected: `"private_key": "[REDACTED]"`,
		},
		{
			name:     "unquoted key with Bearer token value",
			input:    "auth: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig",
			expected: "auth: [REDACTED]",
		},
		{
			name:     "unquoted key with Bearer token value (token keyword)",
			input:    "token=Bearer abc123def456",
			expected: "token=[REDACTED]",
		},
		{
			name:     "URI with sensitive keyword as username (token)",
			input:    "postgres://token:s3cret@db.example.com:5432/mydb",
			expected: "postgres://token:[REDACTED]@db.example.com:5432/mydb",
		},
		{
			name:     "URI with sensitive keyword as username (auth)",
			input:    "https://auth:password123@api.example.com/v1",
			expected: "https://auth:[REDACTED]@api.example.com/v1",
		},
		{
			name:     "URI with sensitive keyword as username (credential)",
			input:    "mysql://credential:hunter2@db.internal:3306/app",
			expected: "mysql://credential:[REDACTED]@db.internal:3306/app",
		},
		{
			name:     "UUIDs are not redacted",
			input:    "failed to process row for pipeline 550e8400-e29b-41d4-a716-446655440000 in table users",
			expected: "failed to process row for pipeline 550e8400-e29b-41d4-a716-446655440000 in table users",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := ScrubString(tc.input)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
