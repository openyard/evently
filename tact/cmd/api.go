package cmd

type CreateFunc func() *DomainModel

type API interface {
	Process(c *Command) error
}
