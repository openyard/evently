package consume_test

import (
	"testing"
	"time"

	"github.com/openyard/evently/async/query/consume"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/tact/es"
	"github.com/stretchr/testify/assert"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNewTracingFilter(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	spanRec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSpanProcessor(spanRec)))

	ctx := testContext()
	tp := otel.GetTracerProvider()
	tracer := tp.Tracer("TestNewTracingFilter")
	context, _ := ctx.Context()
	parentCtx, span := tracer.Start(context, "parent")
	defer span.End()

	sut := consume.NewTracingFilter("my-subscription")
	err := sut.Consume(ctx.WithParent(parentCtx), entries()...)
	assert.NoError(t, err)

	err = exp.ExportSpans(parentCtx, spanRec.Ended())
	assert.NoError(t, err)
	for _, s := range exp.GetSpans().Snapshots() {
		assert.NotEmpty(t, s)
		t.Logf("span=%+v", s.Name())
		assert.NotEmpty(t, s.Parent())
		t.Logf("parent->traceID=%s,spanID=%s", s.Parent().TraceID(), s.Parent().SpanID())
	}
}

func entries() []*es.Entry {
	return []*es.Entry{
		es.NewEntry(123, event.NewEventAt("test-event", "payment-123", time.Now())),
		es.NewEntry(124, event.NewEventAt("test-event", "payment-124", time.Now())),
		es.NewEntry(125, event.NewEventAt("test-event", "payment-125", time.Now())),
	}
}

func testContext() consume.Context {
	return consume.NewContext("my-subscription", []consume.Msg{
		consume.NewMsg("4711", "test-message", []byte(`the message`)),
	})
}
