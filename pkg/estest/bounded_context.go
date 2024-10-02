package estest

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/openyard/evently/event"
	"github.com/openyard/evently/tact/cmd"
)

type BoundedContext struct {
	tb testing.TB
	dm *cmd.DomainModel
}

// NewBoundedContext returns an initialized *estest.BoundedContext with given domain-model
func NewBoundedContext(tb testing.TB, dm *cmd.DomainModel) *BoundedContext {
	tb.Helper()
	return &BoundedContext{tb: tb, dm: dm}
}

// Given applies the given domain events to the model and executes the given func
func (bc *BoundedContext) Given(events ...*event.Event) {
	bc.tb.Helper()
	bc.dm.Load(events)
}

func (bc *BoundedContext) When(c *cmd.Command) (string, time.Time) {
	bc.tb.Helper()
	changes, err := bc.dm.Execute(c)
	if err != nil {
		bc.tb.Errorf("[%T] failed executing command(%+v) - err: %s", bc, c, err)
		return "", time.Time{}
	}
	return changes[len(changes)-1].ID(), changes[len(changes)-1].OccurredAt()
}

func (bc *BoundedContext) Then(expected ...*event.Event) bool {
	var actual, given string
	for _, c := range bc.dm.Changes() {
		marshalJSON, _ := json.MarshalIndent(c, "", "  ")
		actual = fmt.Sprintf("%s", string(marshalJSON))
	}
	for _, c := range expected {
		marshalJSON, _ := json.MarshalIndent(c, "", "  ")
		given = fmt.Sprintf("%s", string(marshalJSON))
	}
	bc.tb.Logf("actual: %+v\n", actual)
	bc.tb.Logf("expected: %+v\n", given)
	return strings.EqualFold(actual, given)
}
