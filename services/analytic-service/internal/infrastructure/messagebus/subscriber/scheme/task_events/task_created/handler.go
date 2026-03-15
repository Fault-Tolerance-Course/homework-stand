package task_created

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"analytic-service/internal/applicaton/service/task/accept_task"
	"analytic-service/internal/pkg/msgbus/subscriber/retry"

	"github.com/gofrs/uuid"
	"github.com/shopspring/decimal"
)

const eventTypeTaskCreated = "task-created"

type TaskCreator interface {
	Create(ctx context.Context, request accept_task.CreateTaskRequest) error
}

type Handler struct {
	creator TaskCreator
}

func NewMessageHandler(creator TaskCreator) *Handler {
	return &Handler{creator: creator}
}

// EventType возвращает тип ивента
func (h *Handler) EventType() string {
	return eventTypeTaskCreated
}

func (h *Handler) HandleEvent(ctx context.Context, payload []byte) error {
	deserialized, err := deserialize(payload)
	if err != nil {
		slog.Error(fmt.Sprintf("Ошибка десереализации сообщения: %s", err.Error()))
		// Malformed payload can never be fixed by retrying — go straight to DLQ.
		return retry.DLQErr(err)
	}

	amount, err := decimal.NewFromString(deserialized.Price)
	if err != nil {
		// Invalid price format is a permanent data error — go straight to DLQ.
		return retry.DLQErr(fmt.Errorf("invalid price %q: %w", deserialized.Price, err))
	}

	if amount.LessThan(decimal.NewFromInt(90)) {
		return errors.New("amount is less than 90")
	}

	return h.creator.Create(ctx, accept_task.NewCreateTaskRequest(
		uuid.FromStringOrNil(deserialized.TaskID),
		deserialized.UserID,
		deserialized.CategoryID,
		deserialized.Status,
		deserialized.Comment,
		deserialized.ExecutionTime,
		deserialized.CreatedAt,
		amount,
	))
}
