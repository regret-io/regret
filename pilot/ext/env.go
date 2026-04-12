package ext

import (
	"fmt"
	"os"
)

// EnvOr returns the environment variable value or a fallback.
func EnvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// EnvUint16 parses an environment variable as uint16 or returns a fallback.
func EnvUint16(key string, fallback uint16) uint16 {
	var n uint16
	if _, err := fmt.Sscanf(os.Getenv(key), "%d", &n); err == nil {
		return n
	}
	return fallback
}
