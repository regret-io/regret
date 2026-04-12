package ext

import (
	"fmt"
	"strings"
	"time"
)

// NowUnixMilli returns the current time as Unix milliseconds.
func NowUnixMilli() int64 {
	return time.Now().UnixMilli()
}

// NowUTC returns the current time formatted as ISO8601 with milliseconds.
func NowUTC() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

// ParseDuration parses a duration string like "3h", "10m", "60s".
// Returns seconds as uint64. Returns nil if parsing fails.
func ParseDuration(s *string) *uint64 {
	if s == nil || *s == "" {
		return nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(*s))
	if err == nil {
		v := uint64(d.Seconds())
		return &v
	}
	var v uint64
	if _, err := fmt.Sscanf(*s, "%d", &v); err == nil {
		return &v
	}
	return nil
}

// ParseDurationSecs parses a duration string and returns seconds.
// Returns 0 if parsing fails.
func ParseDurationSecs(s string) uint64 {
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err == nil {
		return uint64(d.Seconds())
	}
	var v uint64
	if _, err := fmt.Sscanf(s, "%d", &v); err == nil {
		return v
	}
	return 0
}
