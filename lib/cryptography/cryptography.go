package cryptography

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"os"

	"github.com/artie-labs/transfer/lib/typing"
)

// HashValue - Hashes a value using SHA256
func HashValue(value any) any {
	if value == nil {
		return nil
	}

	hash := sha256.New()
	hash.Write([]byte(fmt.Sprint(value)))
	return hex.EncodeToString(hash.Sum(nil))
}

func LoadRSAKey(filePath string) (*rsa.PrivateKey, error) {
	keyBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return ParseRSAPrivateKey(keyBytes)
}

func ParseRSAPrivateKey(keyBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block containing private key")
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %v", err)
	}

	rsaKey, err := typing.AssertType[*rsa.PrivateKey](key)
	if err != nil {
		return nil, err
	}

	return rsaKey, nil
}

func RandomInt64n(n int64) (int64, error) {
	randN, err := rand.Int(rand.Reader, big.NewInt(n))
	if err != nil {
		return 0, fmt.Errorf("failed to generate random number: %w", err)
	}

	return randN.Int64(), nil
}

func buildCipherAEAD(key string) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("key length must be 32 bytes, got %d", len(key))
	}

	cipherBlock, err := aes.NewCipher([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("failed to generate cipher: %w", err)
	}

	return cipher.NewGCM(cipherBlock)
}

type AES256Encryption struct {
	key cipher.AEAD
}

func NewAES256Encryption(key string) (AES256Encryption, error) {
	cipherAEAD, err := buildCipherAEAD(key)
	if err != nil {
		return AES256Encryption{}, err
	}

	return AES256Encryption{key: cipherAEAD}, nil
}

func (ae AES256Encryption) Encrypt(value string) (string, error) {
	nonce := make([]byte, ae.key.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	encrypted := ae.key.Seal(nil, nonce, []byte(value), nil)
	return hex.EncodeToString(encrypted), nil
}

func (ae AES256Encryption) Decrypt(value string) (string, error) {
	nonce := make([]byte, ae.key.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	decrypted, err := ae.key.Open(nil, nonce, []byte(value), nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt value: %w", err)
	}

	return string(decrypted), nil
}
