package datadog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSampleRate(t *testing.T) {
	assert.Equal(t, getSampleRate("foo"), float64(DefaultSampleRate))
	assert.Equal(t, getSampleRate(1.25), float64(DefaultSampleRate))
	assert.Equal(t, getSampleRate(1), float64(1))
	assert.Equal(t, getSampleRate(0.33), 0.33)
	assert.Equal(t, getSampleRate(0), float64(DefaultSampleRate))
	assert.Equal(t, getSampleRate(-0.55), float64(DefaultSampleRate))
}

func TestGetTags(t *testing.T) {
	assert.Equal(t, getTags(nil), []string{})
	assert.Equal(t, getTags([]string{}), []string{})
	assert.Equal(t, getTags([]interface{}{"env:bar", "a:b"}), []string{"env:bar", "a:b"})
}

func TestNewDatadogClient(t *testing.T) {
	client, err := NewDatadogClient(map[string]interface{}{
		Tags: []string{
			"env:production",
		},
		Namespace: "dusty.",
		Sampling:  0.255,
	})

	assert.NoError(t, err, err)
	mtr, isOk := client.(*statsClient)
	assert.True(t, isOk)
	assert.Equal(t, mtr.rate, 0.255, mtr.rate)
	assert.Equal(t, mtr.client.Namespace, "dusty.", mtr.client.Namespace)
	assert.Equal(t, mtr.client.Tags, []string{"env:production"}, mtr.client.Tags)
}
