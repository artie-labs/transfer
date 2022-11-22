package stringutil

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReverse(t *testing.T) {
	val := "hello"
	assert.Equal(t, Reverse(val), "olleh")

	assert.Equal(t, Reverse("alone"), "enola")
}

func TestReverseComplex(t *testing.T) {
	val := "foo12345k321k3okldsadsa"

	assert.Equal(t, Reverse(val), Reverse(Reverse(Reverse(val))))
	assert.Equal(t, val, Reverse(Reverse(val)))
}
