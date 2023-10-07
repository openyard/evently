package example

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/openyard/evently/command"
)

const (
	onboardCustomerCommandName  = "customer/v1.onboardCustomer" // domain/version.name
	activateCustomerCommandName = "customer/v1.activateCustomer"
	blockCustomerCommandName    = "customer/v1.blockCustomer"
)

type OnboardCustomerCommand struct {
	Name      string
	Birthdate time.Time
	Sex       byte
}

func OnboardCustomer(aggregateID, name string, birthdate time.Time, sex byte) *command.Command {
	payload, _ := json.Marshal(&OnboardCustomerCommand{name, birthdate, sex})
	return command.New(onboardCustomerCommandName, aggregateID, command.WithPayload(payload))
}

type ActivateCustomerCommand struct{}

func ActivateCustomer(aggregateID string) *command.Command {
	return command.New(activateCustomerCommandName, aggregateID)
}

type BlockCustomerCommand struct {
	reason string // optional
}

func BlockCustomer(aggregateID string, reason ...string) *command.Command {
	if len(reason) > 0 {
		payload, _ := json.Marshal(&BlockCustomerCommand{strings.Join(reason, ", ")})
		return command.New(blockCustomerCommandName, aggregateID, command.WithPayload(payload))
	}
	return command.New(blockCustomerCommandName, aggregateID)
}
