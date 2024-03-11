package domain

import (
	"github.com/openyard/evently/command"
	"github.com/openyard/evently/pkg/evently"
)

type Factory struct{}

func NewFactory() *Factory {
	return &Factory{}
}

func (cf *Factory) Create() *evently.DomainModel {
	c := &Customer{}
	c.Init(
		"Customer",
		map[string]evently.Transition{
			newCustomerOnboardedEventName: c.onOnboarding,
			customerActivatedEventName:    c.onActivated,
			customerBlockedEventName:      c.onBlocked,
		},
		map[string]command.HandleFunc{
			onboardCustomerCommandName:  c.create,
			activateCustomerCommandName: c.activate,
			blockCustomerCommandName:    c.block,
		},
	)
	return &c.DomainModel
}
