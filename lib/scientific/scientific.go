package scientific

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

func IsScientificNumber(val interface{}) bool {
	str := fmt.Sprint(val)
	// Check if there are 'e' within the string.
	if strings.Contains(str, "e") {
		_, err := strconv.ParseFloat(str, 64)
		return err == nil
	}

	return false
}

func ToSha256(val interface{}) (string, error) {
	if !IsScientificNumber(val) {
		return "", fmt.Errorf("%v is not a scientific number", val)
	}

	var str string
	switch v := val.(type) {
	case float64:
		// Print the whole number and up to 6 decimal places for higher precision.
		str = fmt.Sprintf("%f", v)
	default:
		return "", fmt.Errorf("%v data type is not supported", val)
	}

	hash := sha256.Sum256([]byte(str))
	return hex.EncodeToString(hash[:]), nil
}
