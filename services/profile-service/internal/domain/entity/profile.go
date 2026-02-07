package entity

import (
	"time"

	"profile-service/internal/domain/value_object"
)

type Profile struct {
	UserID        int64
	Name          string
	Email         string
	IsTaskAllowed bool
	CreatedAt     time.Time

	Tariff      value_object.Tariff
	TariffValid bool
}

// NewProfile конструктор для сущности
func NewProfile(userID int64, name, email string) *Profile {
	now := time.Now()
	return &Profile{
		UserID:        userID,
		Name:          name,
		Email:         email,
		IsTaskAllowed: true,
		CreatedAt:     now,
	}
}

func (p *Profile) AllowTask(value bool) {
	p.IsTaskAllowed = value
}

func (p *Profile) WithTariff(taskCount uint64, taskCountValid bool) {
	if !taskCountValid {
		p.Tariff = value_object.TariffBase
		p.TariffValid = false
		return
	}

	p.TariffValid = true
	// если задач [0,5] -> base
	switch {
	case taskCount >= 0 && taskCount <= 5 || !taskCountValid:
		p.Tariff = value_object.TariffBase
	case taskCount > 5:
		p.Tariff = value_object.TariffMax
	default:
		p.Tariff = value_object.TariffUnknown
	}
}
