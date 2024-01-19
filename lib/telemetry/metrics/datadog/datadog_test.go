package datadog

import (
	"github.com/stretchr/testify/assert"
)

func (d *DatadogTestSuite) TestGetSampleRate() {
	assert.Equal(d.T(), getSampleRate("foo"), float64(DefaultSampleRate))
	assert.Equal(d.T(), getSampleRate(1.25), float64(DefaultSampleRate))
	assert.Equal(d.T(), getSampleRate(1), float64(1))
	assert.Equal(d.T(), getSampleRate(0.33), 0.33)
	assert.Equal(d.T(), getSampleRate(0), float64(DefaultSampleRate))
	assert.Equal(d.T(), getSampleRate(-0.55), float64(DefaultSampleRate))
}

func (d *DatadogTestSuite) TestGetTags() {
	assert.Equal(d.T(), getTags(nil), []string{})
	assert.Equal(d.T(), getTags([]string{}), []string{})
	assert.Equal(d.T(), getTags([]interface{}{"env:bar", "a:b"}), []string{"env:bar", "a:b"})
}

func (d *DatadogTestSuite) TestNewDatadogClient() {
	client, err := NewDatadogClient(map[string]interface{}{
		Tags: []string{
			"env:production",
		},
		Namespace: "dusty.",
		Sampling:  0.255,
	})

	assert.NoError(d.T(), err, err)
	mtr, isOk := client.(*statsClient)
	assert.True(d.T(), isOk)
	assert.Equal(d.T(), mtr.rate, 0.255, mtr.rate)
	assert.Equal(d.T(), mtr.client.Namespace, "dusty.", mtr.client.Namespace)
	assert.Equal(d.T(), mtr.client.Tags, []string{"env:production"}, mtr.client.Tags)
}
