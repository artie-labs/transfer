package cryptography

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

const aes256KeySize = 32

// GeneratePassphrase generates a cryptographically random passphrase suitable for AES-256.
// It returns a base64-encoded string of 32 random bytes, preserving the full 256 bits of entropy.
// Use [DecodePassphrase] to recover the raw 32-byte key for use with [Encrypt] and [Decrypt].
func GeneratePassphrase() (string, error) {
	key := make([]byte, aes256KeySize)
	if _, err := rand.Read(key); err != nil {
		return "", fmt.Errorf("failed to generate passphrase: %w", err)
	}

	return base64.StdEncoding.EncodeToString(key), nil
}

// DecodePassphrase decodes a base64-encoded passphrase (as returned by [GeneratePassphrase])
// back into the raw 32-byte key suitable for [Encrypt] and [Decrypt].
func DecodePassphrase(passphrase string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(passphrase)
	if err != nil {
		return nil, fmt.Errorf("failed to decode passphrase: %w", err)
	}

	if err := ensureKeySize(key); err != nil {
		return nil, err
	}

	return key, nil
}

func ensureKeySize(key []byte) error {
	if len(key) != aes256KeySize {
		return fmt.Errorf("key must be 32 bytes, got: %d", len(key))
	}

	return nil
}

// Encrypt encrypts plaintext using AES-256-GCM with the provided key.
// A random nonce is generated and prepended to the returned ciphertext.
func Encrypt(key, plaintext []byte) ([]byte, error) {
	if err := ensureKeySize(key); err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt decrypts ciphertext that was encrypted with [Encrypt] using AES-256-GCM.
// It expects the nonce to be prepended to the ciphertext.
func Decrypt(key, ciphertext []byte) ([]byte, error) {
	if err := ensureKeySize(key); err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short: expected at least %d bytes, got %d", nonceSize, len(ciphertext))
	}

	nonce, encrypted := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}
