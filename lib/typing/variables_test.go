package typing

import "testing"

func TestSupportedDateTimeLayoutsUniqueness(t *testing.T) {
	layouts := make(map[string]bool)
	for _, layout := range supportedDateTimeLayouts {
		if _, ok := layouts[layout]; ok {
			t.Errorf("layout %q is duplicated", layout)
		}
		layouts[layout] = true
	}
}
