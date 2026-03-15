package producer

import (
	"analytic-service/internal/pkg/connector/kafka"

	"github.com/IBM/sarama"
)

// MessageProducer sends messages to Kafka synchronously.
type MessageProducer struct {
	producer sarama.SyncProducer
}

func NewMessageProducer() *MessageProducer {
	return &MessageProducer{producer: kafka.MustSyncProducer()}
}

func (m *MessageProducer) Close() error {
	return m.producer.Close()
}

// SendMessage sends a single message to Kafka.
// Blocks until the broker acknowledges (WaitForAll).
func (m *MessageProducer) SendMessage(msg *sarama.ProducerMessage) error {
	_, _, err := m.producer.SendMessage(msg)
	return err
}
