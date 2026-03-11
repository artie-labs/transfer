package event

import (
	"encoding/base64"

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
		setColumnTypesToString(cols, nil)
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
		setColumnTypesToString(cols, []string{"email"})
		idCol, _ := cols.GetColumn("id")
		assert.Equal(e.T(), typing.Integer, idCol.KindDetails)
	}
	{
		// Column with a non-string type is overridden to string
		cols := columns.NewColumns([]columns.Column{
			columns.NewColumn("id", typing.Integer),
			columns.NewColumn("email", typing.String),
		})
		setColumnTypesToString(cols, []string{"id"})
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
		setColumnTypesToString(cols, []string{"id", "score"})
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
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{}, "")
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// There's a column to hash, but the event does not have any data
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToHash: []string{"super duper"}}, "")
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Hash the column foo (value is set)
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToHash: []string{"foo"}}, "")
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9", "abc": "def"}, data)
		}
		{
			// Hash the column foo (value is nil)
			data, err := transformData(map[string]any{"foo": nil, "abc": "def"}, kafkalib.TopicConfig{ColumnsToHash: []string{"foo"}}, "")
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": nil, "abc": "def"}, data)
		}
	}
	{
		// Encrypting columns
		passphraseString, err := cryptography.GeneratePassphrase()
		assert.NoError(e.T(), err)
		{
			// No columns to encrypt
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{}, passphraseString)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Column to encrypt does not exist in the data
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToEncrypt: []string{"nonexistent"}}, passphraseString)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Encrypt the column foo (value is set) — verify round-trip
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToEncrypt: []string{"foo"}}, passphraseString)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), "def", data["abc"])
			assert.NotEqual(e.T(), "bar", data["foo"])

			ciphertext, err := base64.StdEncoding.DecodeString(data["foo"].(string))
			assert.NoError(e.T(), err)
			key, err := cryptography.DecodePassphrase(passphraseString)
			assert.NoError(e.T(), err)
			decrypted, err := cryptography.Decrypt(key, ciphertext)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), "bar", string(decrypted))
		}
		{
			// Encrypt the column foo (value is nil) — nil should be preserved
			data, err := transformData(map[string]any{"foo": nil, "abc": "def"}, kafkalib.TopicConfig{ColumnsToEncrypt: []string{"foo"}}, passphraseString)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": nil, "abc": "def"}, data)
		}
		{
			// Multiple columns to encrypt
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def", "num": 42}, kafkalib.TopicConfig{ColumnsToEncrypt: []string{"foo", "num"}}, passphraseString)
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), "def", data["abc"])

			for _, col := range []string{"foo", "num"} {
				ciphertext, err := base64.StdEncoding.DecodeString(data[col].(string))
				assert.NoError(e.T(), err)
				key, err := cryptography.DecodePassphrase(passphraseString)
				assert.NoError(e.T(), err)
				_, err = cryptography.Decrypt(key, ciphertext)
				assert.NoError(e.T(), err)
			}
		}
		{
			// Invalid passphrase length should return an error
			_, err := transformData(map[string]any{"foo": "bar"}, kafkalib.TopicConfig{ColumnsToEncrypt: []string{"foo"}}, "too-short")
			assert.ErrorContains(e.T(), err, "failed to encrypt column")
		}
	}
	{
		// Excluding columns
		{
			// No columns to exclude
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToExclude: []string{}}, "")
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Exclude the column foo
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToExclude: []string{"foo"}}, "")
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"abc": "def"}, data)
		}
	}
	{
		// Include columns
		{
			// No columns to include
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToInclude: []string{}}, "")
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Include the column foo
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}}, "")
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar"}, data)
		}
		{
			// include foo, but also artie columns
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def", constants.DeleteColumnMarker: true}, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}}, "")
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", constants.DeleteColumnMarker: true}, data)
		}
		{
			// Includes static columns
			data, err := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}, StaticColumns: []kafkalib.StaticColumn{{Name: "dusty", Value: "mini aussie"}}}, "")
			assert.NoError(e.T(), err)
			assert.Equal(e.T(), map[string]any{"foo": "bar", "dusty": "mini aussie"}, data)
		}
	}
}
