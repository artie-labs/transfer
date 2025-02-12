package kafkalib

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnection_Mechanism(t *testing.T) {
	{
		c := NewConnection(false, false, "", "")
		assert.Equal(t, Plain, c.Mechanism())
	}
	{
		c := NewConnection(false, false, "username", "password")
		assert.Equal(t, ScramSha512, c.Mechanism())

		// Username and password are set but AWS IAM is enabled
		c = NewConnection(true, false, "username", "password")
		assert.Equal(t, AwsMskIam, c.Mechanism())
	}
	{
		c := NewConnection(true, false, "", "")
		assert.Equal(t, AwsMskIam, c.Mechanism())
	}
}

func TestConnection_Dialer(t *testing.T) {
	ctx := t.Context()
	{
		// Plain
		c := NewConnection(false, false, "", "")
		dialer, err := c.Dialer(ctx)
		assert.NoError(t, err)
		assert.Nil(t, dialer.TLS)
		assert.Nil(t, dialer.SASLMechanism)
	}
	{
		// SCRAM enabled with TLS
		c := NewConnection(false, false, "username", "password")
		dialer, err := c.Dialer(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, dialer.TLS)
		assert.NotNil(t, dialer.SASLMechanism)

		// w/o TLS
		c = NewConnection(false, true, "username", "password")
		dialer, err = c.Dialer(ctx)
		assert.NoError(t, err)
		assert.Nil(t, dialer.TLS)
		assert.NotNil(t, dialer.SASLMechanism)
	}
	{
		// AWS IAM w/ TLS
		c := NewConnection(true, false, "", "")
		dialer, err := c.Dialer(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, dialer.TLS)
		assert.NotNil(t, dialer.SASLMechanism)

		// w/o TLS (still enabled because AWS doesn't support not having TLS)
		c = NewConnection(true, true, "", "")
		dialer, err = c.Dialer(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, dialer.TLS)
		assert.NotNil(t, dialer.SASLMechanism)
	}
}
