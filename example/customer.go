package example

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/openyard/evently/command"
	"github.com/openyard/evently/event"
)

type CustomerState string

const (
	CustomerStateOnboarded CustomerState = "onboarded"
	CustomerStateActive                  = "active"
	CustomerStateBlocked                 = "blocked"
)

// Customer represents the domain-model in a customer domain
type Customer struct {
	command.DomainModel
	id        string
	name      string
	birthdate time.Time
	sex       byte
	state     CustomerState
}

func (customer *Customer) create(c *command.Command) error {
	var cmd OnboardCustomerCommand
	_ = json.Unmarshal(c.Payload(), &cmd)
	if customer.Version() > 0 {
		return fmt.Errorf("customer <%s> already exists", c.AggregateID())
	}
	customer.Causes(NewCustomerOnboarded(c.AggregateID(), cmd.Name, cmd.Sex, cmd.Birthdate))
	return nil
}

func (customer *Customer) activate(command *command.Command) error {
	if customer.state == CustomerStateActive {
		// ignore
		return nil
	}
	customer.Causes(CustomerActivated(command.AggregateID()))
	return nil
}

func (customer *Customer) block(command *command.Command) error {
	if customer.state == CustomerStateBlocked {
		// ignore
		return nil
	}
	var cmd BlockCustomerCommand
	_ = json.Unmarshal(command.Payload(), &cmd)
	customer.Causes(CustomerBlocked(command.AggregateID(), strings.Split(cmd.reason, ", ")...))
	return nil
}

func (customer *Customer) onOnboarding(e *event.Event) {
	customer.id = e.AggregateID()

	var onboardedEvent NewCustomerOnboardedEvent
	if err := json.Unmarshal(e.Payload(), &onboardedEvent); err == nil {
		customer.name = onboardedEvent.name
		customer.birthdate = onboardedEvent.birthdate
		customer.sex = onboardedEvent.sex
		customer.state = CustomerStateOnboarded
	}
}

func (customer *Customer) onActivated(_ *event.Event) {
	customer.state = CustomerStateActive
}

func (customer *Customer) onBlocked(_ *event.Event) {
	customer.state = CustomerStateBlocked
}
