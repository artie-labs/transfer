package cryptography

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGeneratePassphrase(t *testing.T) {
	{
		// Passphrase should be a 32-character string.
		passphrase, err := GeneratePassphrase()
		assert.NoError(t, err)
		assert.Len(t, passphrase, 32)
	}
	{
		// Passphrases should be unique across calls.
		p1, err := GeneratePassphrase()
		assert.NoError(t, err)

		p2, err := GeneratePassphrase()
		assert.NoError(t, err)

		assert.NotEqual(t, p1, p2)
	}
}

func TestEncryptDecrypt(t *testing.T) {
	passphrase, err := GeneratePassphrase()
	assert.NoError(t, err)
	key := []byte(passphrase)

	{
		// Round-trip with a normal string.
		plaintext := []byte("hello world")
		ciphertext, err := Encrypt(key, plaintext)
		assert.NoError(t, err)
		assert.NotEqual(t, plaintext, ciphertext)

		decrypted, err := Decrypt(key, ciphertext)
		assert.NoError(t, err)
		assert.Equal(t, plaintext, decrypted)
	}
	{
		// Round-trip with empty plaintext.
		ciphertext, err := Encrypt(key, []byte{})
		assert.NoError(t, err)

		decrypted, err := Decrypt(key, ciphertext)
		assert.NoError(t, err)
		assert.Empty(t, decrypted)
	}
	{
		// Encrypting the same plaintext twice should produce different ciphertexts (random nonce).
		plaintext := []byte("deterministic?")
		ct1, err := Encrypt(key, plaintext)
		assert.NoError(t, err)

		ct2, err := Encrypt(key, plaintext)
		assert.NoError(t, err)

		assert.NotEqual(t, ct1, ct2)
	}
}

func TestDecrypt_WrongKey(t *testing.T) {
	p1, err := GeneratePassphrase()
	assert.NoError(t, err)

	p2, err := GeneratePassphrase()
	assert.NoError(t, err)

	ciphertext, err := Encrypt([]byte(p1), []byte("secret"))
	assert.NoError(t, err)

	_, err = Decrypt([]byte(p2), ciphertext)
	assert.ErrorContains(t, err, "failed to decrypt")
}

func TestDecrypt_InvalidCiphertext(t *testing.T) {
	passphrase, err := GeneratePassphrase()
	assert.NoError(t, err)
	key := []byte(passphrase)

	{
		// Ciphertext shorter than nonce size.
		_, err = Decrypt(key, []byte("short"))
		assert.ErrorContains(t, err, "ciphertext too short")
	}
	{
		// Corrupted ciphertext.
		ciphertext, err := Encrypt(key, []byte("hello"))
		assert.NoError(t, err)

		ciphertext[len(ciphertext)-1] ^= 0xFF
		_, err = Decrypt(key, ciphertext)
		assert.ErrorContains(t, err, "failed to decrypt")
	}
}
