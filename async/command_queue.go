package async

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/openyard/evently/pkg/evently"
	"github.com/openyard/evently/tact/cmd"
)

const defaultQueueTimeout = 400 * time.Millisecond

// CommandHandlers represents a map of CommandHandleFunc
type CommandHandlers map[string]cmd.HandleFunc

type QueueOption func(q *CommandQueue)

// CommandQueue calls registered CommandHandlers through asynchronous Worker Queues
type CommandQueue struct {
	sync.RWMutex

	commandHandler cmd.HandleFunc
	workerPool     chan chan *cmd.Command
	timeout        time.Duration

	logger *log.Logger
}

// NewCommandQueue with max Workers handles incoming commands
// asynchronous with a default timeout of 400ms
func NewCommandQueue(maxWorkers int, opts ...QueueOption) *CommandQueue {
	queue := &CommandQueue{
		workerPool: make(chan chan *cmd.Command, maxWorkers),
		timeout:    defaultQueueTimeout,
		logger:     log.New(log.Writer(), log.Prefix(), log.Flags()),
	}
	for _, opt := range opts {
		opt(queue)
	}
	queue.start(maxWorkers)
	return queue
}

// WithQueueTimeout sets the given duration as timeout in the command queue.
// If the execution of the command lasts longer than the given duration, the
// command will fail with command.ErrTimeout.
// The default timeout is 400ms.
func WithQueueTimeout(d time.Duration) QueueOption {
	return func(q *CommandQueue) {
		q.timeout = d
	}
}

// Register the given command.HandleFunc for 1 to n commands
func (q *CommandQueue) Register(ch cmd.HandleFunc, commands ...string) {
	q.sync(func() {
		//for _, c := range commands {
		q.commandHandler = ch
		//}
	})
}

// start the CommandBus worker
func (q *CommandQueue) start(maxWorkers int) {
	for i := 0; i < maxWorkers; i++ {
		newWorker(q.commandHandler, q.workerPool)
	}
}

// Send given Command to queue
// If the command is not
func (q *CommandQueue) Send(c *cmd.Command) error {
	if os.Getenv("TRACE") != "" {
		start := time.Now()
		defer q.logger.Printf("[%T] send cmd [%s] took [%s]\n", q, c.CommandName(), time.Now().Sub(start))
	}
	go func(c *cmd.Command) {
		workerJobQueue := <-q.workerPool
		workerJobQueue <- c
	}(c)

	select {
	case <-c.Done():
		return nil
	case err := <-c.Error():
		return err
	case <-time.After(q.timeout):
		return evently.Errorf(cmd.ErrTimeout, "ErrTimeout", "timeout! executing cmd [%+v]", c.CommandName())
	}
}

func (q *CommandQueue) sync(f func()) {
	q.Lock()
	defer q.Unlock()
	f()
}
