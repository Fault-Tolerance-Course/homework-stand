package producer

import (
	msgproducer "analytic-service/internal/pkg/msgbus/producer"
)

// Producers holds all Kafka producers used by the message bus.
type Producers struct {
	// RetryAndDLQ is used to forward failed messages to retry / DLQ topics.
	RetryAndDLQ *msgproducer.MessageProducer
}

func NewProducers() Producers {
	return Producers{
		RetryAndDLQ: msgproducer.NewMessageProducer(),
	}
}
