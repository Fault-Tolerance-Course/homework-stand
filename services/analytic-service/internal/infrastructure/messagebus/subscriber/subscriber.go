package subscriber

import (
	"time"

	"analytic-service/config"
	"analytic-service/internal/pkg/msgbus/producer"
	msgsubscriber "analytic-service/internal/pkg/msgbus/subscriber"
	"analytic-service/internal/pkg/msgbus/subscriber/retry"
)

const retryBackoff = 30 * time.Second

// Subscribers holds all Kafka consumers.
type Subscribers struct {
	// TaskEvents consumes the main task-events topic.
	TaskEvents *msgsubscriber.MessageSubscriber
	// TaskEventsRetry consumes the retry topic and re-attempts processing once.
	TaskEventsRetry *msgsubscriber.MessageSubscriber
}

// NewSubscribers wires the two consumers with their routers.
//
// Retry flow (exactly one retry attempt):
//
//	task-events  (group: analytic-group-main)
//	    retryable error    → task-events-retry-30s  [x-deliver-at = now+30s]
//	    DLQErr (permanent) → task-events-dlq        [no delay, immediate]
//
//	task-events-retry-30s  (group: analytic-group-retry)
//	    waits for x-deliver-at, then processes
//	    any error → task-events-dlq  [no delay, terminal]
func NewSubscribers(retryProducer *producer.MessageProducer) Subscribers {
	// Main consumer: retryable → retry-30s (with delay), non-retryable → dlq directly.
	mainRouter := retry.NewMainRouter(
		retryProducer,
		config.TaskEventsRetry30sTopic,
		config.TaskEventsDLQTopic,
		retryBackoff,
	)

	// Retry consumer: all failures are terminal → dlq (no delay).
	terminalRouter := retry.NewTerminalRouter(
		retryProducer,
		config.TaskEventsDLQTopic,
	)

	mainSubscriber := msgsubscriber.NewMainSubscriber(config.TaskEventsTopic).
		WithRetryRouter(mainRouter)

	retrySubscriber := msgsubscriber.NewRetrySubscriber(config.TaskEventsRetry30sTopic).
		WithRetryRouter(terminalRouter)

	return Subscribers{
		TaskEvents:      mainSubscriber,
		TaskEventsRetry: retrySubscriber,
	}
}
