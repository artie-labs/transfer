package ext

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseFromInterfaceDateTime(t *testing.T) {
	now := time.Now().In(time.UTC)
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		et, err := ParseFromInterface(now.Format(supportedDateTimeLayout), DateTimeKindType)
		assert.NoError(t, err)
		assert.Equal(t, et.NestedKind.Type, DateTimeKindType)
		assert.Equal(t, et.String(""), now.Format(supportedDateTimeLayout))
	}
}

func TestParseFromInterfaceTime(t *testing.T) {
	now := time.Now()
	for _, supportedTimeFormat := range SupportedTimeFormatsLegacy {
		et, err := ParseFromInterface(now.Format(supportedTimeFormat), TimeKindType)
		assert.NoError(t, err)
		assert.Equal(t, et.NestedKind.Type, TimeKindType)
		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(t, et.String(""), now.Format(supportedTimeFormat))
	}
}

func TestParseFromInterfaceDate(t *testing.T) {
	now := time.Now()
	for _, supportedDateFormat := range supportedDateFormats {
		et, err := ParseFromInterface(now.Format(supportedDateFormat), DateKindType)
		assert.NoError(t, err)
		assert.Equal(t, et.NestedKind.Type, DateKindType)

		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(t, et.String(""), now.Format(supportedDateFormat))
	}
}
