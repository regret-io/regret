package endpoints

import (
	"encoding/base64"
	"net/http"
	"strings"
)

// BasicAuth returns a middleware that checks Basic auth on POST/PUT/DELETE methods.
// GET and HEAD requests are always allowed (readonly mode).
func BasicAuth(password string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// GET and HEAD are always allowed
			if r.Method == http.MethodGet || r.Method == http.MethodHead {
				next.ServeHTTP(w, r)
				return
			}

			// Check Authorization header
			authHeader := r.Header.Get("Authorization")
			if validateBasicAuth(authHeader, password) {
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("WWW-Authenticate", `Basic realm="regret"`)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
		})
	}
}

func validateBasicAuth(header, expectedPassword string) bool {
	encoded := strings.TrimPrefix(header, "Basic ")
	if encoded == header || encoded == "" {
		return false
	}

	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return false
	}

	creds := string(decoded)
	// Format: "username:password" -- we only check the password
	parts := strings.SplitN(creds, ":", 2)
	if len(parts) != 2 {
		return false
	}
	return parts[1] == expectedPassword
}
