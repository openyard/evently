package command_test

import (
	"testing"

	"github.com/openyard/evently/command"
)

func TestBatch_AddCommands(t *testing.T) {
	sut := command.NewBatch()
	sut.AddCommands([]*command.Command{
		command.New("test-command", "4711"),
		command.New("test-command", "4712"),
		command.New("test-command", "4713"),
		command.New("test-command", "4714"),
	}...)

	t.Logf("batch-id: %s", sut.ID())
	commands := sut.Commands()
	assertEq(t, 4, len(commands))
	assertEq(t, 4096, cap(commands))
}

func assertEq(t *testing.T, x int, a int) {
	t.Helper()
	if x != a {
		t.Errorf("%d != %d", x, a)
	}
}
