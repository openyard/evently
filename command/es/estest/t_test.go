package estest_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/openyard/evently/command/es/estest"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/example"
)

func ExampleWithDomainModel() {
	birthdate, _ := time.Parse("2006-01-02", "1999-06-01")
	customer := (&example.CustomerFactory{}).Create()
	myDomain := estest.WithDomainModel(&testing.T{}, customer)
	changes, _ := myDomain.When(example.OnboardCustomer("4711", "John Doe", birthdate, 'M'))
	lastEvent := changes[0]
	expectedEvent := example.NewCustomerOnboardedAt(
		"4711", "John Doe", 'M', birthdate, lastEvent.OccurredAt(), event.WithID(lastEvent.ID()))
	fmt.Printf("%+v", myDomain.Then(expectedEvent))

	// Output:
	// true
}
