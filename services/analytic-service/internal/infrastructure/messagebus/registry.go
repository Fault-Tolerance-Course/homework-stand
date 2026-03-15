package messagebus

import (
	"context"

	"analytic-service/internal/infrastructure/messagebus/producer"
	"analytic-service/internal/infrastructure/messagebus/subscriber"
	"analytic-service/internal/infrastructure/messagebus/subscriber/scheme/task_events"
	"analytic-service/internal/pkg/closer"
	eventrouter "analytic-service/internal/pkg/event-router"
)

type handlers struct {
	TaskEvents *task_events.MessageHandler
}

// Registry wires Kafka producers, consumers, and business handlers.
type Registry struct {
	handlers    handlers
	producers   producer.Producers
	subscribers subscriber.Subscribers
}

func NewRegistry(router *eventrouter.EventRouter[string, []byte]) *Registry {
	producers := producer.NewProducers()

	registry := &Registry{
		producers:   producers,
		subscribers: subscriber.NewSubscribers(producers.RetryAndDLQ),
		handlers: handlers{
			TaskEvents: task_events.NewMessageHandler(router),
		},
	}

	closer.Add(producers.RetryAndDLQ.Close)
	closer.Add(registry.subscribers.TaskEvents.Close)
	closer.Add(registry.subscribers.TaskEventsRetry.Close)

	return registry
}

// Run starts all consumers in separate goroutines.
// Both consumers use the same business handler — the retry consumer applies the
// same processing logic after the scheduled delivery delay has elapsed.
func (r *Registry) Run(ctx context.Context) {
	go r.subscribers.TaskEvents.Subscribe(ctx, r.handlers.TaskEvents.Handle)
	go r.subscribers.TaskEventsRetry.Subscribe(ctx, r.handlers.TaskEvents.Handle)
}
