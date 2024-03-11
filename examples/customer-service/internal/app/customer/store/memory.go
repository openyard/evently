package store

import (
	"customer/internal/app/customer/report"
	"fmt"
)

type InMemoryCustomerStore struct {
	directory map[string]*report.Customer
}

func NewInMemoryCustomerStore() *InMemoryCustomerStore {
	return &InMemoryCustomerStore{directory: make(map[string]*report.Customer)}
}

func (s *InMemoryCustomerStore) StoreCustomer(customer *report.Customer) error {
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

func (s *InMemoryCustomerStore) ListCustomers() map[string]*report.Customer {
	return s.directory
}

func (s *InMemoryCustomerStore) GetCustomerByID(ID string) *report.Customer {
	c, found := s.directory[ID]
	if !found {
		return nil
	}
	return c
}
