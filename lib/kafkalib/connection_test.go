package kafkalib

import (
	"testing"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/assert"
)

func TestConnection_Mechanism(t *testing.T) {
	{
		c := NewConnection(false, false, "", "", DefaultTimeout)
		assert.Equal(t, Plain, c.Mechanism())
	}
	{
		c := NewConnection(false, false, "username", "password", DefaultTimeout)
		assert.Equal(t, ScramSha512, c.Mechanism())

		// Username and password are set but AWS IAM is enabled
		c = NewConnection(true, false, "username", "password", DefaultTimeout)
		assert.Equal(t, AwsMskIam, c.Mechanism())
	}
	{
		c := NewConnection(true, false, "", "", DefaultTimeout)
		assert.Equal(t, AwsMskIam, c.Mechanism())
	}
	{
		// not setting timeout
		c := NewConnection(false, false, "", "", 0)
		assert.Equal(t, DefaultTimeout, c.timeout)
	}
}

func TestConnection_ClientOptions(t *testing.T) {
	ctx := t.Context()
	brokers := []string{"localhost:9092"}

	{
		// Plain without credentials - should have minimal options (brokers + timeout)
		c := NewConnection(false, false, "", "", DefaultTimeout)
		opts, err := c.ClientOptions(ctx, brokers)
		assert.NoError(t, err)
		assert.NotEmpty(t, opts)               // Should have at least seed brokers and timeout
		assert.GreaterOrEqual(t, len(opts), 2) // brokers + timeout, possibly TLS

		// Plain without credentials and w/o TLS - should have exactly 2 options
		c = NewConnection(false, true, "", "", DefaultTimeout)
		opts, err = c.ClientOptions(ctx, brokers)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(opts)) // brokers + timeout only
	}
	{
		// Plain with credentials (Azure Event Hub style) and TLS - should have SASL and TLS dialer options
		c := NewConnection(false, false, "$ConnectionString", "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test", DefaultTimeout)
		opts, err := c.ClientOptions(ctx, brokers)
		assert.NoError(t, err)
		assert.NotEmpty(t, opts)
		// Should have more options (SASL + TLS dialer)
		assert.GreaterOrEqual(t, len(opts), 4) // brokers, timeout, SASL, dialer

		// Plain with credentials but w/o TLS - should have SASL but no TLS dialer
		c = NewConnection(false, true, "$ConnectionString", "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test", DefaultTimeout)
		opts, err = c.ClientOptions(ctx, brokers)
		assert.NoError(t, err)
		assert.NotEmpty(t, opts)
		assert.GreaterOrEqual(t, len(opts), 3) // brokers, timeout, SASL
	}
	{
		// SCRAM enabled with TLS - should have SASL and Dialer options
		// Note: SCRAM is determined by having username/password but not being Azure Event Hub
		c := NewConnection(false, false, "scramuser", "scrampass", DefaultTimeout)
		opts, err := c.ClientOptions(ctx, brokers)
		assert.NoError(t, err)
		assert.NotEmpty(t, opts)
		// Should have more options (SASL + TLS dialer)
		assert.GreaterOrEqual(t, len(opts), 4) // brokers, timeout, SASL, dialer

		// w/o TLS - should have SASL but no TLS dialer
		c = NewConnection(false, true, "scramuser", "scrampass", DefaultTimeout)
		opts, err = c.ClientOptions(ctx, brokers)
		assert.NoError(t, err)
		assert.NotEmpty(t, opts)
		assert.GreaterOrEqual(t, len(opts), 3) // brokers, timeout, SASL
	}
	{
		// AWS IAM w/ TLS - should have SASL and TLS dialer
		c := NewConnection(true, false, "", "", DefaultTimeout)
		opts, err := c.ClientOptions(ctx, brokers, func(options *awsCfg.LoadOptions) error {
			// Mock AWS credentials for testing
			options.Credentials = credentials.NewStaticCredentialsProvider(
				"test-access-key",
				"test-secret-key",
				"test-session-token",
			)
			return nil
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, opts)
		assert.GreaterOrEqual(t, len(opts), 4) // brokers, timeout, SASL, dialer

		// w/o TLS (still enabled because AWS doesn't support not having TLS)
		c = NewConnection(true, true, "", "", DefaultTimeout)
		opts, err = c.ClientOptions(ctx, brokers, func(options *awsCfg.LoadOptions) error {
			// Mock AWS credentials for testing
			options.Credentials = credentials.NewStaticCredentialsProvider(
				"test-access-key",
				"test-secret-key",
				"test-session-token",
			)
			return nil
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, opts)
		assert.GreaterOrEqual(t, len(opts), 4) // brokers, timeout, SASL, dialer (TLS still forced)
	}
}
