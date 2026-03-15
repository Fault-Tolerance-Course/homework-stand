package retry

import (
	"time"

	"github.com/IBM/sarama"
)

const (
	// HeaderDeliverAt carries the earliest time the message should be processed.
	// Format: time.RFC3339. Absent on first delivery and on DLQ messages.
	HeaderDeliverAt = "x-deliver-at"

	// HeaderRetryCount tracks the number of times this message has been retried.
	// Absent (treated as 0) on first delivery from the main topic.
	HeaderRetryCount = "x-retry-count"

	// HeaderOriginalTopic is the topic the message was first published to.
	// Set only on the first retry hop and never overwritten.
	HeaderOriginalTopic = "x-original-topic"

	// HeaderPreviousTopic is the topic the message was most recently consumed from.
	// Updated on every hop.
	HeaderPreviousTopic = "x-previous-topic"

	// HeaderOriginalPartition is the Kafka partition of the original message.
	HeaderOriginalPartition = "x-original-partition"

	// HeaderOriginalOffset is the Kafka offset of the original message.
	HeaderOriginalOffset = "x-original-offset"

	// HeaderError carries the error message that caused this routing.
	// Updated on every hop with the latest failure reason.
	HeaderError = "x-error"

	// HeaderErrorTime is the RFC3339 timestamp when the last error occurred.
	HeaderErrorTime = "x-error-time"

	// HeaderNonRetryable is set to "true" when a message is routed directly
	// to DLQ due to a non-retryable error (e.g., deserialization failure).
	HeaderNonRetryable = "x-non-retryable"
)

func parseHeaders(headers []*sarama.RecordHeader) map[string]string {
	result := make(map[string]string, len(headers))
	for _, h := range headers {
		if h != nil {
			result[string(h.Key)] = string(h.Value)
		}
	}
	return result
}

func toRecordHeaders(m map[string]string) []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0, len(m))
	for k, v := range m {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	return headers
}

// DeliverAt extracts the delivery timestamp from message headers.
// Returns zero time if the header is absent or unparseable.
func DeliverAt(message *sarama.ConsumerMessage) time.Time {
	for _, h := range message.Headers {
		if h != nil && string(h.Key) == HeaderDeliverAt {
			t, err := time.Parse(time.RFC3339, string(h.Value))
			if err != nil {
				return time.Time{}
			}
			return t
		}
	}
	return time.Time{}
}
