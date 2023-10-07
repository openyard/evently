package async

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/openyard/evently"
	"github.com/openyard/evently/command"
)

const defaultQueueTimeout = 400 * time.Millisecond

// CommandHandlers represents a map of CommandHandleFunc
type CommandHandlers map[string]command.HandleFunc

type QueueOption func(q *CommandQueue)

// CommandQueue calls registered CommandHandlers through asynchronous Worker Queues
type CommandQueue struct {
	sync.RWMutex

	commandHandler command.HandleFunc
	workerPool     chan chan *command.Command
	timeout        time.Duration

	logger *log.Logger
}

// NewCommandQueue with max Workers handles incoming commands
// asynchronous with a default timeout of 400ms
func NewCommandQueue(maxWorkers int, opts ...QueueOption) *CommandQueue {
	queue := &CommandQueue{
		workerPool: make(chan chan *command.Command, maxWorkers),
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
func (q *CommandQueue) Register(ch command.HandleFunc, commands ...string) {
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
func (q *CommandQueue) Send(cmd *command.Command) error {
	if os.Getenv("TRACE") != "" {
		start := time.Now()
		defer q.logger.Printf("[%T] send cmd [%s] took [%s]\n", q, cmd.CommandName(), time.Now().Sub(start))
	}
	go func(c *command.Command) {
		workerJobQueue := <-q.workerPool
		workerJobQueue <- c
	}(cmd)

	select {
	case <-cmd.Done():
		return nil
	case err := <-cmd.Error():
		return err
	case <-time.After(q.timeout):
		return evently.Errorf(command.ErrTimeout, "ErrTimeout", "timeout! executing cmd [%+v]", cmd.CommandName())
	}
}

func (q *CommandQueue) sync(f func()) {
	q.Lock()
	defer q.Unlock()
	f()
}
