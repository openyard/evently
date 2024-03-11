package evently

import "fmt"

// Error specialized with Code and Cause extension. Satisfies error
type Error struct {
	Code  uint
	Name  string
	Text  string
	Cause error
}

// Error return the error in string representation and satisfies the built-in error
func (e *Error) Error() string {
	var cause string
	if e.Cause != nil {
		cause = " (cause: " + e.Cause.Error() + ")"
	}
	return fmt.Sprintf("[%06d-%s] %s%s", e.Code, e.Name, e.Text, cause)
}

// CausedBy add the cause of this specific error
func (e *Error) CausedBy(err error) *Error {
	e.Cause = err
	return e
}

// Unwrap the underlying error (since go 1.13)
func (e *Error) Unwrap() error {
	return e.Cause
}

// Errorf formats according to a format specifier and returns the string
// as a value that satisfies error.
func Errorf(code uint, name, format string, vargs ...interface{}) *Error {
	return &Error{Code: code, Name: name, Text: fmt.Sprintf(format, vargs...)}
}
