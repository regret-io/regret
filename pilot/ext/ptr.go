package ext

// PtrOr returns s if non-nil, otherwise returns a pointer to fallback.
func PtrOr(s *string, fallback string) *string {
	if s != nil {
		return s
	}
	return &fallback
}

// Ptr returns a pointer to v.
func Ptr[T any](v T) *T {
	return &v
}
