package ext

import "testing"

func TestSupportedDateTimeLayoutsUniqueness(t *testing.T) {
	layouts := make(map[string]struct{})
	for _, layout := range supportedDateTimeLayouts {
		if _, ok := layouts[layout]; ok {
			t.Errorf("layout %q is duplicated", layout)
		}
		layouts[layout] = struct{}{}
	}
}
