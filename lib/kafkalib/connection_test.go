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
