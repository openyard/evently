package store

import (
	"customer/internal/app/customer/reporting"

	"fmt"
)

type InMemoryCustomerStore struct {
	directory map[string]*reporting.Customer
}

func NewInMemoryCustomerStore() *InMemoryCustomerStore {
	return &InMemoryCustomerStore{directory: make(map[string]*reporting.Customer)}
}

func (s *InMemoryCustomerStore) StoreCustomer(customer *reporting.Customer) error {
	s.directory[customer.ID] = customer
	return nil
}

func (s *InMemoryCustomerStore) SetCustomerState(ID, state string) error {
	if c, ok := s.directory[ID]; ok {
		c.State = state
		return nil
	}
	return fmt.Errorf("[%T][SetCustomerState] customer <%s> not found", s, ID)
}

func (s *InMemoryCustomerStore) ListCustomers() map[string]*reporting.Customer {
	return s.directory
}

func (s *InMemoryCustomerStore) GetCustomerByID(ID string) *reporting.Customer {
	c, found := s.directory[ID]
	if !found {
		return nil
	}
	return c
}
