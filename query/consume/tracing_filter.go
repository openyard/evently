package consume

import (
	"fmt"

	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"

	"go.opentelemetry.io/otel"
)

var _ Consumer = (*TracingFilter)(nil)

type TracingFilter struct {
	subscriptionID string
}

func NewTracingFilter(subscriptionID string) *TracingFilter {
	return &TracingFilter{subscriptionID: subscriptionID}
}

func (f *TracingFilter) Consume(ctx Context, entries ...*es.Entry) error {
	tp := otel.GetTracerProvider()
	tracer := tp.Tracer(f.subscriptionID)
	for _, e := range entries {
		_, span := tracer.Start(ctx.ctx, spanName(e.GlobalPos(), e.Event()))
		span.End()
	}
	return nil
}

func spanName(pos uint64, event *event.Event) string {
	return fmt.Sprintf("#%d@%s a=%s id=%s", pos, event.Name(), event.AggregateID(), event.ID())
}
