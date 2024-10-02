package consume

import (
	"github.com/openyard/evently/tact/es"
)

type AckFunc func(entries ...*es.Entry)
type NackFunc func(entries ...*es.Entry)
