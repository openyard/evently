package evently_test

import (
	"fmt"
	"github.com/openyard/evently/pkg/evently"
	"github.com/openyard/evently/tact/es"
)

func ExampleError_Error_withCause() {
	err := evently.Errorf(es.ErrReadStreamsFailed, "ErrReadStreamsFailed", "some error").CausedBy(evently.Errorf(es.ErrConcurrentChange, "ErrConcurrentChange", "root cause"))
	fmt.Print(err)

	// Output:
	// [009101-ErrReadStreamsFailed] some error (cause: [009102-ErrConcurrentChange] root cause)
}

func ExampleError_Error_unwrap() {
	err := evently.Errorf(es.ErrReadStreamsFailed, "ErrReadStreamsFailed", "some error").CausedBy(evently.Errorf(es.ErrConcurrentChange, "ErrConcurrentChange", "root cause"))
	fmt.Print(err.Unwrap())

	// Output:
	// [009102-ErrConcurrentChange] root cause
}
