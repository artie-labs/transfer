package cryptography

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
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
	// hash.Hash.Write never returns an error, so we can safely ignore the error from fmt.Fprint.
	_, _ = fmt.Fprint(hash, value)
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
