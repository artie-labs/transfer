package environ

import (
	"fmt"
	"os"
	"strings"
)

func MustGetEnv(envVars ...string) error {
	var invalidParts []string
	for _, envVar := range envVars {
		if os.Getenv(envVar) == "" {
			invalidParts = append(invalidParts, envVar)
		}
	}

	if len(invalidParts) > 0 {
		return fmt.Errorf("required environment variables %q are not set", strings.Join(invalidParts, ", "))
	}

	return nil
}
