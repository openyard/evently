package estest_test

import (
	"fmt"
	"github.com/openyard/evently/tact/es"
	"testing"
	"time"

	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/estest"
	"github.com/openyard/evently/tact/cmd"
)

func ExampleBoundedContext() {
	stamp, _ := time.Parse("2006-01-02", "1999-06-01")
	fakeDomain := estest.NewBoundedContext(&testing.T{}, newFakeModel())
	fakeDomain.Given(event.NewEventAt("fakeEvent", "1337", stamp, event.WithEventType(event.DomainEvent)))
	ID, occurred := fakeDomain.When(cmd.New("fakeCommand", "1377", cmd.WithExpectedVersion(1)))
	expectedEvent := event.NewEventAt("fakeEvent", "1337", occurred,
		event.WithEventType(event.DomainEvent), event.WithID(ID))
	fmt.Printf("%+v", fakeDomain.Then(expectedEvent))

	// Output:
	// true
}

type fakeModel struct {
	cmd.DomainModel
	events uint8
}

func newFakeModel() *cmd.DomainModel {
	fm := &fakeModel{}
	fm.Init("fake",
		map[string]es.Transition{
			"fakeEvent": fm.handle,
		}, map[string]cmd.HandleFunc{
			"fakeCommand": fm.process,
		})
	return &fm.DomainModel
}

func (fm *fakeModel) handle(_ *event.Event) {
	fm.events++
}

func (fm *fakeModel) process(_ *cmd.Command) error {
	fm.Causes(event.NewEventAt("fakeEvent", "1337", time.Now(), event.WithEventType(event.DomainEvent)))
	return nil
}
