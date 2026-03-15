package retry

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

// Producer sends a single Kafka message.
type Producer interface {
	SendMessage(msg *sarama.ProducerMessage) error
}

// Router routes failed messages to the next topic in the pipeline.
//
// Use NewMainRouter for the main topic consumer:
//
//	task-events → [retryable error]     → retry-30s  (x-deliver-at = now+backoff)
//	task-events → [DLQErr / permanent]  → dlq        (no delay, immediate)
//
// Use NewTerminalRouter for the retry topic consumer:
//
//	retry-30s → [any error] → dlq  (no delay, terminal)
type Router struct {
	producer  Producer
	nextTopic string        // destination for retryable errors
	dlqTopic  string        // destination for non-retryable errors
	withDelay bool          // whether to attach x-deliver-at to nextTopic messages
	backoff   time.Duration // delay magnitude for retry messages
}

// NewMainRouter creates a router for the main topic consumer.
// Retryable errors go to retryTopic with a backoff delay.
// Non-retryable errors (DLQErr) bypass retry and go directly to dlqTopic.
func NewMainRouter(producer Producer, retryTopic, dlqTopic string, backoff time.Duration) *Router {
	return &Router{
		producer:  producer,
		nextTopic: retryTopic,
		dlqTopic:  dlqTopic,
		withDelay: true,
		backoff:   backoff,
	}
}

// NewTerminalRouter creates a router for the retry topic consumer.
// All failures — retryable and non-retryable — go to dlqTopic with no delay.
func NewTerminalRouter(producer Producer, dlqTopic string) *Router {
	return &Router{
		producer:  producer,
		nextTopic: dlqTopic,
		dlqTopic:  dlqTopic,
		withDelay: false,
	}
}

// Route forwards a failed message based on the error type.
//   - DLQErr (non-retryable): routed directly to dlqTopic, no delay.
//   - Any other error: routed to nextTopic (with delay if configured).
//
// Returns nil when forwarding succeeded — the caller should commit the offset.
// Returns an error when forwarding failed — the caller must NOT commit the offset.
func (r *Router) Route(ctx context.Context, message *sarama.ConsumerMessage, cause error) error {
	nonRetryable := IsNonRetryable(cause)
	targetTopic := r.nextTopic
	withDelay := r.withDelay

	if nonRetryable {
		targetTopic = r.dlqTopic
		withDelay = false
		slog.Warn("routing message to DLQ (non-retryable error)",
			"topic", message.Topic,
			"partition", message.Partition,
			"offset", message.Offset,
			"error", cause.Error(),
		)
	} else {
		slog.Info("routing message to next topic",
			"topic", message.Topic,
			"partition", message.Partition,
			"offset", message.Offset,
			"target", targetTopic,
			"error", cause.Error(),
		)
	}

	msg, err := r.buildMessage(message, targetTopic, cause, withDelay, nonRetryable)
	if err != nil {
		return fmt.Errorf("build retry message: %w", err)
	}

	if err = r.producer.SendMessage(msg); err != nil {
		return fmt.Errorf("send to %s: %w", targetTopic, err)
	}

	return nil
}

func (r *Router) buildMessage(
	src *sarama.ConsumerMessage,
	targetTopic string,
	cause error,
	withDelay bool,
	nonRetryable bool,
) (*sarama.ProducerMessage, error) {
	headers := parseHeaders(src.Headers)

	// Preserve the true origin — set only on the first routing hop.
	if _, exists := headers[HeaderOriginalTopic]; !exists {
		headers[HeaderOriginalTopic] = src.Topic
		headers[HeaderOriginalPartition] = strconv.Itoa(int(src.Partition))
		headers[HeaderOriginalOffset] = strconv.Itoa(int(src.Offset))
	}

	// Track immediate source on every hop.
	headers[HeaderPreviousTopic] = src.Topic

	// Increment retry counter (diagnostic only — not used for routing decisions).
	prevCount, _ := strconv.Atoi(headers[HeaderRetryCount])
	headers[HeaderRetryCount] = strconv.Itoa(prevCount + 1)

	// Record the error that caused this routing — always the latest failure.
	headers[HeaderError] = cause.Error()
	headers[HeaderErrorTime] = time.Now().Format(time.RFC3339)

	if nonRetryable {
		headers[HeaderNonRetryable] = "true"
	}

	// Set or clear the delivery timestamp.
	if withDelay {
		headers[HeaderDeliverAt] = time.Now().Add(r.backoff).Format(time.RFC3339)
	} else {
		// DLQ messages carry no delivery delay — operators replay them manually.
		delete(headers, HeaderDeliverAt)
	}

	return &sarama.ProducerMessage{
		Topic:   targetTopic,
		Key:     sarama.ByteEncoder(src.Key),
		Value:   sarama.ByteEncoder(src.Value),
		Headers: toRecordHeaders(headers),
	}, nil
}
