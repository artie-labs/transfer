package metrics

import (
	"github.com/stretchr/testify/assert"
)

func (m *MetricsTestSuite) TestGetSampleRate() {
	assert.Equal(m.T(), getSampleRate("foo"), float64(DefaultSampleRate))
	assert.Equal(m.T(), getSampleRate(1.25), float64(DefaultSampleRate))
	assert.Equal(m.T(), getSampleRate(1), float64(1))
	assert.Equal(m.T(), getSampleRate(0.33), 0.33)
	assert.Equal(m.T(), getSampleRate(0), float64(DefaultSampleRate))
	assert.Equal(m.T(), getSampleRate(-0.55), float64(DefaultSampleRate))
}

func (m *MetricsTestSuite) TestGetTags() {
	assert.Equal(m.T(), getTags(nil), []string{})
	assert.Equal(m.T(), getTags([]string{}), []string{})
	assert.Equal(m.T(), getTags([]interface{}{"env:bar", "a:b"}), []string{"env:bar", "a:b"})
}

func (m *MetricsTestSuite) TestNewDatadogClient() {
	var err error

	m.ctx, err = NewDatadogClient(m.ctx, map[string]interface{}{
		Tags: []string{
			"env:production",
		},
		Namespace: "dusty.",
		Sampling:  0.255,
		// Cannot test datadogAddr (addr is private)
	})

	assert.NoError(m.T(), err, err)
	mtr := FromContext(m.ctx).(*statsClient)

	assert.Equal(m.T(), mtr.rate, 0.255, mtr.rate)
	assert.Equal(m.T(), mtr.client.Namespace, "dusty.", mtr.client.Namespace)
	assert.Equal(m.T(), mtr.client.Tags, []string{"env:production"}, mtr.client.Tags)
}
