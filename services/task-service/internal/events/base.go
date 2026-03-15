package events

type Base[T any] struct {
	EventType string `json:"event_type"`
	EntityID  string `json:"entity_id"`
	Payload   T      `json:"payload"`
}
