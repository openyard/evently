package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/openyard/evently/event"
	"github.com/openyard/evently/tact/cmd"
)

type State string

const (
	StateOnboarded State = "onboarded"
	StateActive          = "active"
	StateBlocked         = "blocked"
)

// Customer represents the domain-model in a customer domain
type Customer struct {
	cmd.DomainModel
	id        string
	name      string
	birthdate time.Time
	sex       byte
	state     State
}

func (c *Customer) create(command *cmd.Command) error {
	var occ onboardCustomerCommand
	_ = json.Unmarshal(command.Payload(), &occ)
	if c.Version() > 0 {
		return fmt.Errorf("customer <%s> already exists", command.AggregateID())
	}
	c.Causes(onboarded(command.AggregateID(), occ.Name, occ.Sex, occ.Birthdate))
	return nil
}

func (c *Customer) activate(command *cmd.Command) error {
	if c.state == StateActive {
		// ignore
		return nil
	}
	c.Causes(activated(command.AggregateID()))
	return nil
}

func (c *Customer) block(command *cmd.Command) error {
	if c.state == StateBlocked {
		// ignore
		return nil
	}
	var bcc blockCustomerCommand
	_ = json.Unmarshal(command.Payload(), &bcc)
	c.Causes(blocked(command.AggregateID(), strings.Split(bcc.Reason, ", ")...))
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
