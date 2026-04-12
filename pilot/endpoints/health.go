package endpoints

import (
	"net/http"
)

// HealthHandler returns {"status":"ok"}.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// MetricsPlaceholderHandler returns a placeholder metrics response.
func MetricsPlaceholderHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("# regret-pilot metrics\n"))
}
