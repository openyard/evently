package cmd

import "github.com/openyard/evently/pkg/uuid"

const (
	BatchSizeXS BatchSize = 1024
	BatchSizeS            = 2048
	BatchSizeM            = 4096
	BatchSizeL            = 8192
	BatchSizeXL           = 16384

	defaultBatchSize = BatchSizeM
)

type BatchSize uint16

// Batch ...
type Batch struct {
	id       string
	commands []*Command
}

// BatchOption ...
type BatchOption func(*Batch)

// NewBatch creates a new batch with default batch-size of 8192
func NewBatch() *Batch {
	return &Batch{
		id:       uuid.NewV4().String(),
		commands: make([]*Command, 0, defaultBatchSize),
	}
}

// WithBatchSize uses the given batch-size as the max. possible quantity for the underlying batch-process
func WithBatchSize(batchSize BatchSize) BatchOption {
	return func(b *Batch) {
		b.commands = make([]*Command, 0, batchSize)
	}
}

func (b *Batch) AddCommands(cmd ...*Command) {
	b.commands = append(b.commands, cmd...)
}

func (b *Batch) ID() string {
	return b.id
}

func (b *Batch) Commands() []*Command {
	return b.commands
}
