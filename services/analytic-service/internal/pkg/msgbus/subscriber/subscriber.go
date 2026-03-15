package subscriber

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"analytic-service/internal/pkg/connector/kafka"
	"analytic-service/internal/pkg/connector/kafka/consumer"
	"analytic-service/internal/pkg/msgbus/subscriber/retry"

	"github.com/IBM/sarama"
)

// MessageSubscriber wraps a TopicConsumer and adds:
//   - Delivery delay support (x-deliver-at header)
//   - Automatic retry/DLQ routing on processing failure
type MessageSubscriber struct {
	subscriber  *consumer.TopicConsumer
	retryRouter *retry.Router
}

// NewMainSubscriber creates a subscriber for the main topic.
// Uses the primary consumer group (analytic-group-main).
func NewMainSubscriber(topic string) *MessageSubscriber {
	return &MessageSubscriber{subscriber: consumer.NewTopicConsumer(topic, kafka.MustConsumerGroup())}
}

// NewRetrySubscriber creates a subscriber for the retry topic.
// Uses the dedicated retry consumer group (analytic-group-retry) with
// MaxProcessingTime tuned to support the 30-second delivery delay.
func NewRetrySubscriber(topic string) *MessageSubscriber {
	return &MessageSubscriber{subscriber: consumer.NewTopicConsumer(topic, kafka.MustRetryConsumerGroup())}
}

// WithRetryRouter attaches a retry router that handles failed messages.
func (m *MessageSubscriber) WithRetryRouter(router *retry.Router) *MessageSubscriber {
	m.retryRouter = router
	return m
}

func (m *MessageSubscriber) Subscribe(ctx context.Context, handler consumer.MessageHandler) {
	m.subscriber.Subscribe(ctx, m.handle(handler))
}

func (m *MessageSubscriber) Close() error {
	return m.subscriber.Close()
}

func (m *MessageSubscriber) Stop() {
	m.subscriber.Stop()
}

func (m *MessageSubscriber) Errors() <-chan error {
	return m.subscriber.Errors()
}

// handle wraps the user-provided handler with:
//  1. Delivery delay waiting (for retry-topic messages with x-deliver-at header)
//  2. Error routing to retry topic or DLQ
func (m *MessageSubscriber) handle(handler consumer.MessageHandler) consumer.MessageHandler {
	return func(ctx context.Context, session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
		// ── Step 1: respect scheduled delivery time ──────────────────────────
		// Retry messages carry x-deliver-at to prevent immediate re-processing.
		// We sleep here — safe because:
		//   - The retry consumer group uses MaxProcessingTime=60s (> 30s backoff)
		//   - Sarama sends heartbeats in a background goroutine; the session
		//     remains alive during the sleep (session timeout=90s > 30s backoff)
		if deliverAt := retry.DeliverAt(message); !deliverAt.IsZero() {
			if delay := time.Until(deliverAt); delay > 0 {
				select {
				case <-session.Context().Done():
					// Rebalance or shutdown during wait — do NOT commit the
					// offset; the message will be re-read in the next session.
					slog.Warn(fmt.Sprintf(
						"session cancelled during delivery delay, message will be reprocessed: "+
							"topic=%s partition=%d offset=%d",
						message.Topic, message.Partition, message.Offset,
					))
					return session.Context().Err()
				case <-time.After(delay):
					// ready
				}
			}
		}

		// ── Step 2: invoke business handler ──────────────────────────────────
		err := handler(ctx, session, message)
		if err == nil {
			return nil // success — caller (ConsumeClaim) will commit the offset
		}

		// ── Step 3: route failed message to retry topic or DLQ ───────────────
		if m.retryRouter == nil {
			// No retry configured — propagate; caller will NOT commit.
			return err
		}

		slog.Warn(fmt.Sprintf(
			"message processing failed, routing: topic=%s partition=%d offset=%d error=%v",
			message.Topic, message.Partition, message.Offset, err,
		))

		if routeErr := m.retryRouter.Route(ctx, message, err); routeErr != nil {
			// Routing itself failed (e.g., Kafka temporarily unavailable).
			// Return the error so ConsumeClaim exits without committing.
			// The consumer loop will reconnect and re-read this message.
			slog.Error(fmt.Sprintf("failed to route message: %v", routeErr))
			return routeErr
		}

		// Routing succeeded: the message is now in retry or DLQ.
		// Return nil so the caller commits the original offset and moves on.
		return nil
	}
}
