package retry

import "errors"

// NonRetryableError marks an error as not eligible for retry.
// Messages that produce this error are routed directly to DLQ, bypassing
// the retry topic and its associated delivery delay.
type NonRetryableError struct {
	cause error
}

func (e *NonRetryableError) Error() string { return e.cause.Error() }
func (e *NonRetryableError) Unwrap() error { return e.cause }

// DLQErr wraps err to signal that the failed message should be routed
// directly to DLQ without going through the retry topic.
//
// Use for errors that are permanent and cannot be resolved by retrying:
// deserialization failures, schema violations, malformed payloads.
//
// Example:
//
//	if err := json.Unmarshal(payload, &event); err != nil {
//	    return retry.DLQErr(err)
//	}
func DLQErr(err error) error {
	if err == nil {
		return nil
	}
	return &NonRetryableError{cause: err}
}

// IsNonRetryable reports whether err was wrapped with DLQErr.
func IsNonRetryable(err error) bool {
	var nre *NonRetryableError
	return errors.As(err, &nre)
}
