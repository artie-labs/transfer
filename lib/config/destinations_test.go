package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabricks_DSN(t *testing.T) {
	d := Databricks{
		Host:                "foo",
		HttpPath:            "/api/def",
		Port:                443,
		Catalog:             "catalogName",
		PersonalAccessToken: "pat",
	}

	assert.Equal(t, "token:pat@foo:443/api/def?catalog=catalogName", d.DSN())
}
