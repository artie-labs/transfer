package datadog

import (
	"reflect"
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
	assert.Equal(t, getTags([]any{"env:bar", "a:b"}), []string{"env:bar", "a:b"})
}

func TestNewDatadogClient(t *testing.T) {
	client, err := NewDatadogClient(map[string]any{
		Tags: []string{
			"env:production",
		},
		Namespace: "dusty.",
		Sampling:  0.255,
	})

	assert.NoError(t, err)
	mtr, isOk := client.(*statsClient)
	assert.True(t, isOk)
	assert.Equal(t, 0.255, mtr.rate)

	assert.Equal(t, "dusty.", reflect.ValueOf(*mtr.client).FieldByName("namespace").String())
	tagsField := reflect.ValueOf(*mtr.client).FieldByName("tags")
	assert.Equal(t, 1, tagsField.Len())
	assert.Equal(t, "env:production", tagsField.Index(0).String())
}
