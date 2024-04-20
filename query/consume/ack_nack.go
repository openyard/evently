package consume

import "github.com/openyard/evently/command/es"

type AckFunc func(entries ...*es.Entry)
type NackFunc func(entries ...*es.Entry)
