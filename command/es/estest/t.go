package estest

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/openyard/evently/command"
	"github.com/openyard/evently/event"
)

type T struct {
	*testing.T
	dm *command.DomainModel
}

// WithDomainModel returns an initialized *estest.T with given domain-model
func WithDomainModel(t *testing.T, dm *command.DomainModel) *T {
	return &T{t, dm}
}

// Given applies the given domain events to the model and executes the given func
func (t *T) Given(events ...*event.Event) *T {
	t.dm.Load(events)
	return t
}

func (t *T) When(c *command.Command) ([]*event.Event, error) {
	return t.dm.Execute(c)
}

func (t *T) Then(expected ...*event.Event) bool {
	var actual, given string
	for _, c := range t.dm.Changes() {
		marshalJSON, _ := json.MarshalIndent(c, "", "  ")
		actual = fmt.Sprintf("%s", string(marshalJSON))
	}
	for _, c := range expected {
		marshalJSON, _ := json.MarshalIndent(c, "", "  ")
		given = fmt.Sprintf("%s", string(marshalJSON))
	}
	return strings.EqualFold(actual, given)
}
