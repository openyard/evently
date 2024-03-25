package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/openyard/evently/command"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/evently"
)

type State string

const (
	StateOnboarded State = "onboarded"
	StateActive          = "active"
	StateBlocked         = "blocked"
)

// Customer represents the domain-model in a customer domain
type Customer struct {
	evently.DomainModel
	id        string
	name      string
	birthdate time.Time
	sex       byte
	state     State
}

func (c *Customer) create(command *command.Command) error {
	var cmd onboardCustomerCommand
	_ = json.Unmarshal(command.Payload(), &cmd)
	if c.Version() > 0 {
		return fmt.Errorf("customer <%s> already exists", command.AggregateID())
	}
	c.Causes(onboarded(command.AggregateID(), cmd.Name, cmd.Sex, cmd.Birthdate))
	return nil
}

func (c *Customer) activate(command *command.Command) error {
	if c.state == StateActive {
		// ignore
		return nil
	}
	c.Causes(activated(command.AggregateID()))
	return nil
}

func (c *Customer) block(command *command.Command) error {
	if c.state == StateBlocked {
		// ignore
		return nil
	}
	var cmd blockCustomerCommand
	_ = json.Unmarshal(command.Payload(), &cmd)
	c.Causes(blocked(command.AggregateID(), strings.Split(cmd.Reason, ", ")...))
	return nil
}

func (c *Customer) onOnboarding(e *event.Event) {
	c.id = e.AggregateID()

	var onboardedEvent OnboardedEvent
	if err := json.Unmarshal(e.Payload(), &onboardedEvent); err == nil {
		c.name = onboardedEvent.Name
		c.birthdate = onboardedEvent.Birthdate
		c.sex = onboardedEvent.Sex
		c.state = StateOnboarded
	}
}

func (c *Customer) onActivated(_ *event.Event) {
	c.state = StateActive
}

func (c *Customer) onBlocked(_ *event.Event) {
	c.state = StateBlocked
}
