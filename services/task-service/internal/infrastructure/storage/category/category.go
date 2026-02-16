package category

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"task-service/internal/domain/entity"
	"task-service/internal/infrastructure/storage/category/dao"

	"github.com/samber/lo"
	"github.com/shopspring/decimal"
)

type Storage struct {
	mtx        sync.RWMutex
	categories map[string]dao.Category
}

func NewStorage() *Storage {
	return &Storage{categories: map[string]dao.Category{}}
}

func (s *Storage) IsLoaded() bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return len(s.categories) != 0
}

func (s *Storage) LoadCategories(_ context.Context, filePath string) error {
	// имитация тяжелой загрузки
	time.Sleep(30 * time.Second)

	// Читаем и парсим файл
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	var result struct {
		Categories []dao.Category `json:"categories"`
	}

	if err = json.Unmarshal(data, &result); err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.categories = lo.SliceToMap(result.Categories, func(category dao.Category) (string, dao.Category) {
		return category.ID, category
	})

	return nil
}

func (s *Storage) GetCategory(_ context.Context, id string) (entity.Category, error) {
	s.mtx.RLock()
	category := s.categories[id]
	s.mtx.RUnlock()

	return entity.Category{
		ID:    category.ID,
		Desc:  category.Desc,
		Title: category.Title,
		Price: decimal.NewFromFloat(category.Price),
	}, nil
}

func (s *Storage) GetAllCategories(_ context.Context) ([]entity.Category, error) {
	var categories []entity.Category

	s.mtx.RLock()
	for _, category := range s.categories {
		categories = append(categories, entity.Category{
			ID:    category.ID,
			Desc:  category.Desc,
			Title: category.Title,
			Price: decimal.NewFromFloat(category.Price),
		})
	}
	s.mtx.RUnlock()

	return categories, nil
}
