package ext

import "encoding/json"

// MustJSON marshals v to JSON bytes. Returns nil on error.
func MustJSON(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return data
}
