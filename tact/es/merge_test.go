package es_test

import (
	"fmt"
	"testing"

	"github.com/openyard/evently/event"
	"github.com/openyard/evently/tact/es"
	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	res := es.Merge(createSingleChanges())
	assert.Equal(t, 10, len(res), "res: %+v", res)

	res = es.Merge(createBundledChanges())
	assert.Equal(t, 10, len(res), "res: %+v", res)
}

func createSingleChanges() []es.Change {
	changes := make([]es.Change, 0)
	for i := 0; i < 10; i++ {
		changes = append(changes, []es.Change{
			es.NewChange(fmt.Sprintf("my-stream-%d", i+1), 0, event.NewDomainEvent("my-first-event", "4711")),
			es.NewChange(fmt.Sprintf("my-stream-%d", i+1), 1, event.NewDomainEvent("my-second-event", "4711")),
			es.NewChange(fmt.Sprintf("my-stream-%d", i+1), 2, event.NewDomainEvent("my-third-event", "4711")),
		}...)
	}
	return changes
}

func createBundledChanges() []es.Change {
	changes := make([]es.Change, 0)
	for i := 0; i < 10; i++ {
		changes = append(changes,
			es.NewChange(fmt.Sprintf("my-stream-%d", i+1), 0,
				event.NewDomainEvent("my-first-event", "4711"),
				event.NewDomainEvent("my-second-event", "4711"),
				event.NewDomainEvent("my-third-event", "4711")),
		)
	}
	return changes
}
