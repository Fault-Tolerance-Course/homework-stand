package task_events

import (
	"context"
	"encoding/json"

	eventrouter "analytic-service/internal/pkg/event-router"

	"github.com/IBM/sarama"
)

type BaseEvent struct {
	EventType string          `json:"event_type"`
	EntityID  string          `json:"entity_id"`
	Payload   json.RawMessage `json:"payload"`
}

type MessageHandler struct {
	router *eventrouter.EventRouter[string, []byte]
}

func NewMessageHandler(router *eventrouter.EventRouter[string, []byte]) *MessageHandler {
	return &MessageHandler{router: router}
}

func (h *MessageHandler) Handle(ctx context.Context, _ sarama.ConsumerGroupSession, kafkaMessage *sarama.ConsumerMessage) error {
	var base BaseEvent
	if err := json.Unmarshal(kafkaMessage.Value, &base); err != nil {
		return err
	}

	return h.router.HandleEvent(ctx, base.EventType, base.Payload)
}
