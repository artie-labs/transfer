package ext

import (
	"time"

	"github.com/stretchr/testify/assert"
)

func (e *ExtTestSuite) TestExtTimeIsValid() {
	{
		ext := ExtendedTime{
			Time: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		assert.True(e.T(), ext.IsValid())
	}
	{
		ext := ExtendedTime{
			Time: time.Date(20020, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		assert.False(e.T(), ext.IsValid())
	}
	{
		ext := ExtendedTime{
			Time: time.Date(-1300, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		assert.False(e.T(), ext.IsValid())
	}
}
