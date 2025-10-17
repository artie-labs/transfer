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

func TestConnection_Dialer(t *testing.T) {
	ctx := t.Context()
	{
		// Plain
		c := NewConnection(false, false, "", "", DefaultTimeout)
		dialer, err := c.Dialer(ctx)
		assert.NoError(t, err)
		assert.Nil(t, dialer.TLS)
		assert.Nil(t, dialer.SASLMechanism)
	}
	{
		// SCRAM enabled with TLS
		c := NewConnection(false, false, "username", "password", DefaultTimeout)
		dialer, err := c.Dialer(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, dialer.TLS)
		assert.NotNil(t, dialer.SASLMechanism)

		// w/o TLS
		c = NewConnection(false, true, "username", "password", DefaultTimeout)
		dialer, err = c.Dialer(ctx)
		assert.NoError(t, err)
		assert.Nil(t, dialer.TLS)
		assert.NotNil(t, dialer.SASLMechanism)
	}
	{
		// AWS IAM w/ TLS
		c := NewConnection(true, false, "", "", DefaultTimeout)
		dialer, err := c.Dialer(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, dialer.TLS)
		assert.NotNil(t, dialer.SASLMechanism)

		// w/o TLS (still enabled because AWS doesn't support not having TLS)
		c = NewConnection(true, true, "", "", DefaultTimeout)
		dialer, err = c.Dialer(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, dialer.TLS)
		assert.NotNil(t, dialer.SASLMechanism)
	}
}

func TestConnection_ClientOptions(t *testing.T) {
	ctx := t.Context()
	brokers := []string{"localhost:9092"}

	{
		// Plain - should have minimal options (brokers + timeout)
		c := NewConnection(false, false, "", "", DefaultTimeout)
		opts, err := c.ClientOptions(ctx, brokers)
		assert.NoError(t, err)
		assert.NotEmpty(t, opts) // Should have at least seed brokers and timeout
	}
	{
		// SCRAM enabled with TLS - should have SASL and Dialer options
		c := NewConnection(false, false, "username", "password", DefaultTimeout)
		opts, err := c.ClientOptions(ctx, brokers)
		assert.NoError(t, err)
		assert.NotEmpty(t, opts)
		// Should have more options (SASL + TLS dialer)
		assert.GreaterOrEqual(t, len(opts), 3) // brokers, timeout, SASL, dialer

		// w/o TLS - should have SASL but no TLS dialer
		c = NewConnection(false, true, "username", "password", DefaultTimeout)
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
