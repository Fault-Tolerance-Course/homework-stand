package task_created

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"task-service/config"
	"task-service/internal/domain/entity"
	"task-service/internal/events"

	"task-service/internal/pkg/event"

	"task-service/internal/pkg/pipe"

	"github.com/gofrs/uuid"
)

type TaskCreatedEvent struct {
	TaskID        string        `json:"task_id"`
	UserID        int64         `json:"user_id"`
	CategoryID    string        `json:"category_id"`
	Status        string        `json:"status"`
	Comment       string        `json:"comment"`
	ExecutionTime time.Duration `json:"execution_time"`
	CreatedAt     time.Time     `json:"created_at"`
	Price         string        `json:"price"`
}

func New(task *entity.Task) pipe.Func[event.Events] {
	return func(ctx context.Context, batch event.Events) (event.Events, error) {
		// формируем событие
		baseEvent := events.Base[TaskCreatedEvent]{
			EventType: "task-created",
			EntityID:  strconv.Itoa(int(task.UserID)),
			Payload: TaskCreatedEvent{
				TaskID:        task.ID.String(),
				UserID:        task.UserID,
				CategoryID:    task.CategoryID,
				Status:        string(task.Status),
				Comment:       task.Comment,
				ExecutionTime: task.ExecutionTime,
				CreatedAt:     task.CreatedAt,
				Price:         task.Price.String(),
			},
		}
		// формируем событие
		body, err := json.Marshal(baseEvent)
		if err != nil {
			return nil, err
		}

		correlationID, _ := uuid.NewV7()
		// заголовки в сообщении любые
		headers := map[string]string{
			"x-correlation-id": correlationID.String(),
			"x-app-name":       "task-service",
			"x-event-type":     "task-created",
		}

		// это выносится в общие функции
		headersRaw, err := json.Marshal(headers)
		if err != nil {
			return nil, err
		}

		return append(batch, event.Event{
			Key:     event.Raw(task.ID.String()),
			Body:    body,
			Headers: headersRaw,
			Schema:  config.TaskEventsTopic,
		}), nil
	}
}
