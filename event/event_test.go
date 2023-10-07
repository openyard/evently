package event_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/openyard/evently/event"
)

func TestEvent_MarshalJSON(t *testing.T) {
	myEvent := struct {
		Foo string
		Baz string
	}{
		Foo: "bar",
		Baz: "123",
	}
	eventPayload, err := json.MarshalIndent(myEvent, "", "  ")
	if err != nil {
		t.Errorf("marshal error: %s", err)
	}
	occurredAt, _ := time.Parse(time.RFC3339Nano, "2023-08-25T12:58:31.923778168Z")
	e := event.NewEventAt("test-event", "4711", occurredAt,
		event.WithID("0815"),
		event.WithPayload(eventPayload),
		event.WithEventType(event.DomainEvent))
	b, err := e.MarshalJSON()
	if err != nil {
		t.Errorf("marshal error: %s", err)
	}
	if !strings.EqualFold(expected, string(b)) {
		t.Errorf("marshal mismatch: \n<%s> \n!= \n<%s>", expected, b)
	}
}

func TestEvent_UnmarshalJSON(t *testing.T) {
	var e event.Event
	if err := e.UnmarshalJSON([]byte(expected)); err != nil {
		t.Errorf("unmarshal error: %s", err)
	}
	strings.EqualFold("DomainEvent", string(e.Kind()))
	strings.EqualFold("4711", e.AggregateID())
	strings.EqualFold("0815", e.ID())
	strings.EqualFold("test-event", e.Name())
	strings.EqualFold("2023-08-25T12:58:31.923778168Z", e.OccurredAt().String())
	bytes.EqualFold([]byte("ewogICJGb28iOiAiYmFyIiwKICAiQmF6IjogIjEyMyIKfQ=="), e.Payload())

}

var expected = `{
  "AggregateID": "4711",
  "ID": "0815",
  "Kind": "DomainEvent",
  "Name": "test-event",
  "OccurredAt": "2023-08-25T12:58:31.923778168Z",
  "Payload": "ewogICJGb28iOiAiYmFyIiwKICAiQmF6IjogIjEyMyIKfQ=="
}`
