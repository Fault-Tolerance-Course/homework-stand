package get_profile

import (
	"context"
	"log/slog"

	"profile-service/internal/domain/entity"

	"google.golang.org/grpc/status"
)

type ProfileProvider interface {
	GetProfile(ctx context.Context, userID int64) (*entity.Profile, error)
}

type TaskProvider interface {
	GetUserTaskCount(ctx context.Context, userID int64) (uint64, error)
}

type Service struct {
	profileProvider ProfileProvider
	taskProvider    TaskProvider
}

func NewService(profileProvider ProfileProvider, taskProvider TaskProvider) *Service {
	return &Service{profileProvider: profileProvider, taskProvider: taskProvider}
}

func (s *Service) GetProfile(ctx context.Context, userID int64) (*entity.Profile, error) {
	// получаем профиль
	profile, err := s.profileProvider.GetProfile(ctx, userID)
	if err != nil {
		return nil, err
	}

	// получаем кол-во задач
	taskCount, err := s.taskProvider.GetUserTaskCount(ctx, userID)
	if err != nil {
		// логируем ошибку, но не выходим
		slog.Error("error getting task count, falling back to base",
			"error", err.Error(),
			"code", status.Code(err).String(),
		)
	}

	profile.WithTariff(taskCount, err == nil)

	return profile, nil
}
