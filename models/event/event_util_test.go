package event

import (
	"encoding/base64"
	"encoding/json"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (e *EventsTestSuite) TestSetColumnTypesToString() {
	{
		// No columns - all types unchanged
		cols := columns.NewColumns([]columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("name", typing.String),
		})
		SetColumnTypesToString(cols, nil)
		idCol, _ := cols.GetColumn("id")
		assert.Equal(e.T(), typing.Integer, idCol.KindDetails)
		nameCol, _ := cols.GetColumn("name")
		assert.Equal(e.T(), typing.String, nameCol.KindDetails)
	}
	{
		// Column does not exist - no-op, no panic
		cols := columns.NewColumns([]columns.Column{
			columns.NewColumn("id", typing.Integer),
		})
		SetColumnTypesToString(cols, []string{"email"})
		idCol, _ := cols.GetColumn("id")
		assert.Equal(e.T(), typing.Integer, idCol.KindDetails)
	}
	{
		// Column with a non-string type is overridden to string
		cols := columns.NewColumns([]columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("email", typing.String),
		})
		SetColumnTypesToString(cols, []string{"id"})
		idCol, _ := cols.GetColumn("id")
		assert.Equal(e.T(), typing.String, idCol.KindDetails)
		emailCol, _ := cols.GetColumn("email")
		assert.Equal(e.T(), typing.String, emailCol.KindDetails)
	}
	{
		// Multiple columns - all overridden, others unchanged
		cols := columns.NewColumns([]columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("score", typing.Float),
			columns.NewColumn("active", typing.Boolean),
			columns.NewColumn("name", typing.String),
		})
		SetColumnTypesToString(cols, []string{"id", "score"})
		idCol, _ := cols.GetColumn("id")
		assert.Equal(e.T(), typing.String, idCol.KindDetails)
		scoreCol, _ := cols.GetColumn("score")
		assert.Equal(e.T(), typing.String, scoreCol.KindDetails)
		activeCol, _ := cols.GetColumn("active")
		assert.Equal(e.T(), typing.Boolean, activeCol.KindDetails)
		nameCol, _ := cols.GetColumn("name")
		assert.Equal(e.T(), typing.String, nameCol.KindDetails)
	}
}

func (e *EventsTestSuite) TestBuildPrimaryKeys() {
	{
		// No primary keys override
		pks := buildPrimaryKeys(kafkalib.TopicConfig{}, map[string]any{}, nil)
		assert.Empty(e.T(), pks)
	}
	{
		// Primary keys override
		pks := buildPrimaryKeys(kafkalib.TopicConfig{PrimaryKeysOverride: []string{"id"}}, map[string]any{}, nil)
		assert.Equal(e.T(), []string{"id"}, pks)
	}
	{
		// Include primary keys
		pks := buildPrimaryKeys(kafkalib.TopicConfig{IncludePrimaryKeys: []string{"id"}}, map[string]any{}, nil)
		assert.Equal(e.T(), []string{"id"}, pks)
	}
	{
		// Include primary keys and primary keys override
		pks := buildPrimaryKeys(kafkalib.TopicConfig{PrimaryKeysOverride: []string{}, IncludePrimaryKeys: []string{"id2"}}, map[string]any{"id": "123", "id2": "456"}, nil)
		assert.ElementsMatch(e.T(), []string{"id", "id2"}, pks)
	}
}

func (e *EventsTestSuite) TestTransformData() {
	{
		// Hashing columns
		{
			// No columns to hash
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{}, nil, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// There's a column to hash, but the event does not have any data
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToHash: []string{"super duper"}}, nil, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Hash the column foo (value is set)
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToHash: []string{"foo"}}, nil, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9", "abc": "def"}, data)
		}
		{
			// Hash the column foo (value is nil)
			data, err := transformData(map[string]any{"foo": nil, "abc": "def"}, kafkalib.TopicConfig{ColumnsToHash: []string{"foo"}}, nil, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": nil, "abc": "def"}, data)
		}
		{
			// Hash the column foo with a customer-provided salt (HMAC-SHA256).
			data, err := transformData(
				map[string]any{"foo": "bar", "abc": "def"},
				kafkalib.TopicConfig{ColumnsToHash: []string{"foo"}, ColumnsToHashSalt: "pepper"},
				nil, nil,
			)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "4bb78279b8aabc9aeec1279237934fb12e061512e5c626116dfdcec82d42ff73", "abc": "def"}, data)
			// Salted hash must differ from the unsalted hash for the same input.
			assert.NotEqual(e.T(), "fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9", data["foo"])
		}
	}
	{
		// Encrypting columns
		passphraseString, err := cryptography.GeneratePassphrase()
		assert.NoError(e.T(), err)
		key, err := cryptography.DecodePassphrase(passphraseString, true)
		assert.NoError(e.T(), err)
		{
			// No columns to encrypt
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{}, key, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Column to encrypt does not exist in the data
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToEncrypt: []string{"nonexistent"}}, key, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Encrypt the column foo (value is set) — verify round-trip
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToEncrypt: []string{"foo"}}, key, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), "def", data["abc"])
			assert.NotEqual(e.T(), "bar", data["foo"])

			ciphertext, err := base64.StdEncoding.DecodeString(data["foo"].(string))
			assert.NoError(e.T(), err)
			decrypted, err := cryptography.Decrypt(key, ciphertext)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), "bar", string(decrypted))
		}
		{
			// Encrypt the column foo (value is nil) — nil should be preserved
			data, err := transformData(map[string]any{"foo": nil, "abc": "def"}, kafkalib.TopicConfig{ColumnsToEncrypt: []string{"foo"}}, key, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": nil, "abc": "def"}, data)
		}
		{
			// Multiple columns to encrypt
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def", "num": 42}, kafkalib.TopicConfig{ColumnsToEncrypt: []string{"foo", "num"}}, key, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), "def", data["abc"])

			for _, col := range []string{"foo", "num"} {
				ciphertext, err := base64.StdEncoding.DecodeString(data[col].(string))
				assert.NoError(e.T(), err)
				_, err = cryptography.Decrypt(key, ciphertext)
				assert.NoError(e.T(), err)
			}
		}
		{
			// Invalid key size should return an error from Encrypt
			_, err := transformData(map[string]any{"foo": "bar"}, kafkalib.TopicConfig{ColumnsToEncrypt: []string{"foo"}}, []byte("too-short"), nil)
			assert.ErrorContains(e.T(), err, "failed to encrypt column")
		}
	}
	{
		// Excluding columns
		{
			// No columns to exclude
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToExclude: []string{}}, nil, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Exclude the column foo
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToExclude: []string{"foo"}}, nil, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"abc": "def"}, data)
		}
	}
	{
		// Include columns
		{
			// No columns to include
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToInclude: []string{}}, nil, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Include the column foo
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}}, nil, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar"}, data)
		}
		{
			// include foo, but also artie columns
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def", constants.DeleteColumnMarker: true}, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}}, nil, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", constants.DeleteColumnMarker: true}, data)
		}
		{
			// Includes static columns
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}, StaticColumns: []kafkalib.StaticColumn{{Name: "dusty", Value: "mini aussie"}}}, nil, nil)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "dusty": "mini aussie"}, data)
		}
	}
}

func (e *EventsTestSuite) TestTransformData_EncryptJSONBColumns() {
	passphraseString, err := cryptography.GeneratePassphrase()
	assert.NoError(e.T(), err)
	key, err := cryptography.DecodePassphrase(passphraseString, true)
	assert.NoError(e.T(), err)

	{
		// JSONB column is encrypted and round-trips correctly
		jsonbValue := map[string]any{"nested": "value", "count": float64(42)}
		data, err := transformData(
			map[string]any{"payload": jsonbValue, "name": "test"},
			kafkalib.TopicConfig{}, key, []string{"payload"},
		)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "test", data["name"])
		assert.NotEqual(e.T(), jsonbValue, data["payload"])

		ciphertext, err := base64.StdEncoding.DecodeString(data["payload"].(string))
		assert.NoError(e.T(), err)
		decrypted, err := cryptography.Decrypt(key, ciphertext)
		assert.NoError(e.T(), err)

		var result map[string]any
		assert.NoError(e.T(), json.Unmarshal(decrypted, &result))
		assert.Equal(e.T(), jsonbValue, result)
	}
	{
		// Nil JSONB value is preserved
		data, err := transformData(
			map[string]any{"payload": nil, "name": "test"},
			kafkalib.TopicConfig{}, key, []string{"payload"},
		)
		assert.NoError(e.T(), err)
		assert.Nil(e.T(), data["payload"])
		assert.Equal(e.T(), "test", data["name"])
	}
	{
		// Non-JSONB columns are not affected
		data, err := transformData(
			map[string]any{"payload": map[string]any{"key": "val"}, "name": "test"},
			kafkalib.TopicConfig{}, key, []string{"payload"},
		)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "test", data["name"])
	}
	{
		// JSONB column that doesn't exist in data is skipped
		data, err := transformData(
			map[string]any{"name": "test"},
			kafkalib.TopicConfig{}, key, []string{"nonexistent"},
		)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), map[string]any{"name": "test"}, data)
	}
	{
		// Works alongside ColumnsToEncrypt
		data, err := transformData(
			map[string]any{"payload": map[string]any{"key": "val"}, "secret": "hidden", "name": "test"},
			kafkalib.TopicConfig{ColumnsToEncrypt: []string{"secret"}}, key, []string{"payload"},
		)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "test", data["name"])

		// Both should be encrypted (base64 strings)
		_, err = base64.StdEncoding.DecodeString(data["payload"].(string))
		assert.NoError(e.T(), err)
		_, err = base64.StdEncoding.DecodeString(data["secret"].(string))
		assert.NoError(e.T(), err)
	}
	{
		// Empty jsonbColumnsToEncrypt slice does nothing
		data, err := transformData(
			map[string]any{"payload": map[string]any{"key": "val"}, "name": "test"},
			kafkalib.TopicConfig{}, key, []string{},
		)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), map[string]any{"payload": map[string]any{"key": "val"}, "name": "test"}, data)
	}
}
