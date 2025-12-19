package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseVersion(t *testing.T) {
	{
		version, err := parseVersion("PostgreSQL 16.2 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 7.3.1 20180712 (Red Hat 7.3.1-12), 64-bit")
		assert.NoError(t, err)
		assert.Equal(t, 16, version.MajorVersion)
		assert.Equal(t, 2, version.MinorVersion)
	}
	{
		version, err := parseVersion("PostgreSQL 13.15 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 8.5.0 20210514 (Red Hat 8.5.0-20), 64-bit")
		assert.NoError(t, err)
		assert.Equal(t, 13, version.MajorVersion)
		assert.Equal(t, 15, version.MinorVersion)
	}
	{
		version, err := parseVersion("PostgreSQL 14.10 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 7.3.1 20180712 (Red Hat 7.3.1-12), 64-bit")
		assert.NoError(t, err)
		assert.Equal(t, 14, version.MajorVersion)
		assert.Equal(t, 10, version.MinorVersion)
	}
	{
		version, err := parseVersion("PostgreSQL 15.4 on x86_64-pc-linux-gnu, compiled by x86_64-pc-linux-gnu-gcc (GCC) 9.5.0, 64-bit")
		assert.NoError(t, err)
		assert.Equal(t, 15, version.MajorVersion)
		assert.Equal(t, 4, version.MinorVersion)
	}
	{
		_, err := parseVersion("hi")
		assert.Error(t, err)
	}
	{
		_, err := parseVersion("PostgreSQL")
		assert.Error(t, err)
	}
}
