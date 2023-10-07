package example

import "github.com/openyard/evently/command"

type CustomerFactory struct{}

func (cf *CustomerFactory) Create() *command.DomainModel {
	c := &Customer{}
	c.Init(
		"Customer",
		map[string]command.Transition{
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
