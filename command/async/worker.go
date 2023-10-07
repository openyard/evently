package async

import (
	"log"
	"os"

	"github.com/openyard/evently/command"
	"github.com/openyard/evently/pkg/uuid"
)

type worker struct {
	id             string
	commandHandler command.HandleFunc
	jobChannel     chan *command.Command
	workerPool     chan chan *command.Command

	logger *log.Logger
}

func newWorker(ch command.HandleFunc, wp chan chan *command.Command) {
	id := uuid.NewV4().String()
	w := &worker{
		id:             id,
		commandHandler: ch,
		jobChannel:     make(chan *command.Command, len(wp)*10),
		workerPool:     wp,
		logger:         log.New(log.Writer(), log.Prefix(), log.Flags()),
	}
	w.start()
}

func (w *worker) start() {
	go func() {
		for {
			w.workerPool <- w.jobChannel

			job := <-w.jobChannel
			if err := w.commandHandler.Handle(job); err != nil {
				w.logger.Printf("[%T] command execution failed: %s", w, err)
				job.Failed(err)
				return
			}
			if os.Getenv("TRACE") != "" {
				w.logger.Printf("[%T][%s] command execution done: %s(%s) for %s", w, w.id, job.CommandName(), job.CommandID(), job.AggregateID())
			}
			job.Executed()
		}
	}()
}
