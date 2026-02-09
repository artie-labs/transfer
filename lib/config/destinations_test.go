package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabricks_Validate(t *testing.T) {
	{
		// No auth configured
		d := Databricks{}
		assert.ErrorContains(t, d.Validate(), "only one of personalAccessToken or clientID/clientSecret must be provided")
	}
	{
		// Both PAT and OAuth M2M configured
		d := Databricks{PersonalAccessToken: "pat", ClientID: "id", ClientSecret: "secret"}
		assert.ErrorContains(t, d.Validate(), "only one of personalAccessToken or clientID/clientSecret must be provided")
	}
	{
		// OAuth M2M missing clientSecret
		d := Databricks{ClientID: "id"}
		assert.ErrorContains(t, d.Validate(), "OAuth M2M requires clientSecret")
	}
	{
		// OAuth M2M missing clientID
		d := Databricks{ClientSecret: "secret"}
		assert.ErrorContains(t, d.Validate(), "OAuth M2M requires clientID")
	}
	{
		// PAT is valid
		d := Databricks{PersonalAccessToken: "pat"}
		assert.NoError(t, d.Validate())
	}
	{
		// OAuth M2M is valid
		d := Databricks{ClientID: "id", ClientSecret: "secret"}
		assert.NoError(t, d.Validate())
	}
}

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
