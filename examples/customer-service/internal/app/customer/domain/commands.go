package domain

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/openyard/evently/tact/cmd"
)

const (
	onboardCustomerCommandName  = "customer/v1.onboardCustomer" // domain/version.name
	activateCustomerCommandName = "customer/v1.activateCustomer"
	blockCustomerCommandName    = "customer/v1.blockCustomer"
)

type onboardCustomerCommand struct {
	Name      string
	Birthdate time.Time
	Sex       byte
}

func OnboardCustomer(aggregateID, name string, birthdate time.Time, sex byte) *cmd.Command {
	payload, _ := json.Marshal(&onboardCustomerCommand{name, birthdate, sex})
	return cmd.New(onboardCustomerCommandName, aggregateID, cmd.WithPayload(payload))
}

type activateCustomerCommand struct{}

func ActivateCustomer(aggregateID string) *cmd.Command {
	return cmd.New(activateCustomerCommandName, aggregateID)
}

type blockCustomerCommand struct {
	Reason string // optional
}

func BlockCustomer(aggregateID string, reason ...string) *cmd.Command {
	if len(reason) > 0 {
		payload, _ := json.Marshal(&blockCustomerCommand{strings.Join(reason, ", ")})
		return cmd.New(blockCustomerCommandName, aggregateID, cmd.WithPayload(payload))
	}
	return cmd.New(blockCustomerCommandName, aggregateID)
}
