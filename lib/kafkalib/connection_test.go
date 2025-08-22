package kafkalib

import (
	"testing"

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
		opts, err := c.ClientOptions(ctx, brokers)
		assert.NoError(t, err)
		assert.NotEmpty(t, opts)
		assert.GreaterOrEqual(t, len(opts), 4) // brokers, timeout, SASL, dialer

		// w/o TLS (still enabled because AWS doesn't support not having TLS)
		c = NewConnection(true, true, "", "", DefaultTimeout)
		opts, err = c.ClientOptions(ctx, brokers)
		assert.NoError(t, err)
		assert.NotEmpty(t, opts)
		assert.GreaterOrEqual(t, len(opts), 4) // brokers, timeout, SASL, dialer (TLS still forced)
	}
}
