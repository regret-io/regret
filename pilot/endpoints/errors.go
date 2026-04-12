package endpoints

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// ApiError is a structured HTTP error.
type ApiError struct {
	Code    int
	Message string
}

func (e *ApiError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Message)
}

// NotFound creates a 404 error.
func NotFound(msg string) *ApiError {
	return &ApiError{Code: http.StatusNotFound, Message: msg}
}

// Conflict creates a 409 error.
func Conflict(msg string) *ApiError {
	return &ApiError{Code: http.StatusConflict, Message: msg}
}

// BadRequest creates a 400 error.
func BadRequest(msg string) *ApiError {
	return &ApiError{Code: http.StatusBadRequest, Message: msg}
}

// InternalError creates a 500 error from an error.
func InternalError(err error) *ApiError {
	return &ApiError{Code: http.StatusInternalServerError, Message: err.Error()}
}

// WriteError writes a JSON error response.
func WriteError(w http.ResponseWriter, err *ApiError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(err.Code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Message})
}

// WriteJSON writes a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
