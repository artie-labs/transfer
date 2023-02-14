package typing

import (
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDiffTargNil(t *testing.T) {
	source := map[string]KindDetails{"foo": Invalid}
	srcKeyMissing, targKeyMissing := Diff(source, nil)
	assert.Equal(t, len(srcKeyMissing), 0)
	assert.Equal(t, len(targKeyMissing), 1)
}

func TestDiffSourceNil(t *testing.T) {
	targ := map[string]KindDetails{"foo": Invalid}
	srcKeyMissing, targKeyMissing := Diff(nil, targ)
	assert.Equal(t, len(srcKeyMissing), 1)
	assert.Equal(t, len(targKeyMissing), 0)
}

func TestDiffBasic(t *testing.T) {
	source := map[string]KindDetails{
		"a": Integer,
	}

	srcKeyMissing, targKeyMissing := Diff(source, source)
	assert.Equal(t, len(srcKeyMissing), 0)
	assert.Equal(t, len(targKeyMissing), 0)
}

func TestDiffDelta1(t *testing.T) {
	source := map[string]KindDetails{
		"a": String,
		"b": Boolean,
		"c": Struct,
	}

	target := map[string]KindDetails{
		"aa": String,
		"b":  Boolean,
		"cc": String,
	}

	srcKeyMissing, targKeyMissing := Diff(source, target)
	assert.Equal(t, len(srcKeyMissing), 2)  // Missing aa, cc
	assert.Equal(t, len(targKeyMissing), 2) // Missing aa, cc
}

func TestDiffDelta2(t *testing.T) {
	source := map[string]KindDetails{
		"a":  String,
		"aa": String,
		"b":  Boolean,
		"c":  Struct,
		"d":  String,
		"CC": String,
		"cC": String,
		"Cc": String,
	}

	target := map[string]KindDetails{
		"aa": String,
		"b":  Boolean,
		"cc": String,
		"CC": String,
		"dd": String,
	}

	srcKeyMissing, targKeyMissing := Diff(source, target)
	assert.Equal(t, len(srcKeyMissing), 1, srcKeyMissing)   // Missing dd
	assert.Equal(t, len(targKeyMissing), 3, targKeyMissing) // Missing a, c, d
}

func TestCopyColMap(t *testing.T) {
	oneMap := map[string]KindDetails{
		"hello":      String,
		"created_at": NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType),
		"updated_at": NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType),
	}

	anotherMap := CopyColMap(oneMap)
	delete(anotherMap, "hello")
	assert.NotEqual(t, oneMap, anotherMap)
}
