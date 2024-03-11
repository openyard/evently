package evently_test

import (
	"fmt"
	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/pkg/evently"
)

func ExampleError_Error_withCause() {
	err := evently.Errorf(es.ErrReadStreamFailed, "ErrReadStreamFailed", "some error").CausedBy(evently.Errorf(es.ErrConcurrentChange, "ErrConcurrentChange", "root cause"))
	fmt.Print(err)

	// Output:
	// [009101-ErrReadStreamFailed] some error (cause: [009102-ErrConcurrentChange] root cause)
}

func ExampleError_Error_unwrap() {
	err := evently.Errorf(es.ErrReadStreamFailed, "ErrReadStreamFailed", "some error").CausedBy(evently.Errorf(es.ErrConcurrentChange, "ErrConcurrentChange", "root cause"))
	fmt.Print(err.Unwrap())

	// Output:
	// [009102-ErrConcurrentChange] root cause
}
