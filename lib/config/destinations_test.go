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

func TestMSSQL_DSN(t *testing.T) {
	{
		// Without ReadOnlyIntent
		m := MSSQL{
			Host:     "localhost",
			Port:     1433,
			Username: "sa",
			Password: "password",
			Database: "testdb",
		}
		assert.Equal(t, "sqlserver://sa:password@localhost:1433?database=testdb", m.DSN())
	}
	{
		// With ReadOnlyIntent and a database
		m := MSSQL{
			Host:           "localhost",
			Port:           1433,
			Username:       "sa",
			Password:       "password",
			Database:       "testdb",
			ReadOnlyIntent: true,
		}
		assert.Equal(t, "sqlserver://sa:password@localhost:1433?applicationintent=ReadOnly&database=testdb", m.DSN())
	}
	{
		// ReadOnlyIntent true but empty database — should NOT add applicationintent
		m := MSSQL{
			Host:           "localhost",
			Port:           1433,
			Username:       "sa",
			Password:       "password",
			ReadOnlyIntent: true,
		}
		assert.Equal(t, "sqlserver://sa:password@localhost:1433?database=", m.DSN())
	}
	{
		// Password with special characters
		m := MSSQL{
			Host:           "db.example.com",
			Port:           1433,
			Username:       "admin",
			Password:       "p@ss:word/123",
			Database:       "prod",
			ReadOnlyIntent: true,
		}
		dsn := m.DSN()
		assert.Contains(t, dsn, "applicationintent=ReadOnly")
		assert.Contains(t, dsn, "database=prod")
		assert.Contains(t, dsn, "db.example.com:1433")
	}
}
