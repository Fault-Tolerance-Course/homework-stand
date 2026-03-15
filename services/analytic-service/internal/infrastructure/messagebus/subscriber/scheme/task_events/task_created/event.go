package task_created

import (
	"encoding/json"
	"time"
)

type TaskCreated struct {
	TaskID        string        `json:"task_id"`
	UserID        int64         `json:"user_id"`
	CategoryID    string        `json:"category_id"`
	Comment       string        `json:"comment"`
	Status        string        `json:"status"`
	Price         string        `json:"price"`
	ExecutionTime time.Duration `json:"execution_time"`
	CreatedAt     time.Time     `json:"created_at"`
}

func deserialize(payload []byte) (TaskCreated, error) {
	var taskCreated TaskCreated
	err := json.Unmarshal(payload, &taskCreated)
	if err != nil {
		return TaskCreated{}, err
	}

	return taskCreated, nil
}
