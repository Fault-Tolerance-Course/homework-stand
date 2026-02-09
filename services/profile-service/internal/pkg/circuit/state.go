package circuit

import (
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"profile-service/internal/pkg/circuit/metrics"

	"github.com/samber/lo"
	"github.com/sony/gobreaker/v2"
)

type cbStateByHandlers struct {
	cfg            Config
	handlersConfig map[string]HandlerConfig
	handlersState  map[string]*internalState
	defaultState   *internalState
}

type internalState struct {
	cfg     MainConfig
	breaker *gobreaker.CircuitBreaker[any]
}

func newInternalState(config MainConfig, name string) *internalState {
	type stateChangeEvent struct {
		from, to gobreaker.State
	}

	stateChangedCh := make(chan stateChangeEvent, 1)

	go func(stateChangedCh <-chan stateChangeEvent) {
		timeSinceLastChange := time.Now()
		for event := range stateChangedCh {
			metrics.ObserveStateTransitionDuration(name, event.from, event.to, time.Now().Sub(timeSinceLastChange).Seconds())
			timeSinceLastChange = time.Now()
		}
	}(stateChangedCh)

	settings := gobreaker.Settings{
		Name:         name,
		MaxRequests:  config.MaxRequests,
		Interval:     config.Interval,
		Timeout:      config.OpenTimeout,
		BucketPeriod: config.BucketPeriod,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Срабатываем, если количество ошибок превышает порог
			// Тут смотрим как на последовательные ошибки, так и на общий процент в окне

			enoughRequests := counts.Requests > 10 // берем от 10 запросов

			consecutiveFailuresExceeded := counts.ConsecutiveFailures >= config.ThresholdConsecutive

			var failurePercentage uint32
			if counts.Requests != 0 {
				failurePercentage = counts.TotalFailures * 100 / counts.Requests
			}

			totalFailuresExceeded := failurePercentage > config.ThresholdPercentage

			shouldOpen := enoughRequests && (consecutiveFailuresExceeded || totalFailuresExceeded)

			slog.Info("circuit stats on failed request",
				"количество запросов", counts.Requests,
				"всего запросов", counts.Requests,
				"всего ошибок", counts.TotalFailures,
				"всего успехов", counts.TotalSuccesses,
				"последовательные ошибки", counts.ConsecutiveFailures,
				"последовательные успехи", counts.ConsecutiveSuccesses,
				"процент ошибок от общего числа запросов", failurePercentage,
			)
			if shouldOpen {
				slog.Info("circuit opened condition triggered",
					"достаточно ли запросов для выборки", lo.Ternary(enoughRequests, "да", "нет"),
					"превышено кол-во последовательных ошибок", lo.Ternary(consecutiveFailuresExceeded, "да", "нет"),
					"превышен общий процент ошибок от числа запросов", lo.Ternary(totalFailuresExceeded, "да", "нет"),
				)
			}

			return shouldOpen
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			slog.Info(fmt.Sprintf("circuit breaker '%s' changed state: %s → %s", name, from, to))
			stateChangedCh <- stateChangeEvent{from: from, to: to}
		},
		IsSuccessful: func(err error) bool {
			return !triggerOnError(err, config.FailureCodes, config.failureCodeSet())
		},
	}

	state := &internalState{
		breaker: gobreaker.NewCircuitBreaker[any](settings),
		cfg:     config,
	}

	runtime.AddCleanup(state, func(ch chan stateChangeEvent) {
		close(ch)
	}, stateChangedCh)

	return state
}

func (s cbStateByHandlers) get(method string) (_ *internalState, enabled bool) {
	st, ok := s.handlersState[method]
	if ok {
		return st, true
	}

	isEnabled := s.isHandlerEnabled(method)
	if !isEnabled {
		return nil, false
	}

	if !*s.cfg.Default.SeparatePerHandlerEnabled && s.defaultState != nil {
		return s.defaultState, true
	}

	return nil, true
}

func (s cbStateByHandlers) isHandlerEnabled(method string) bool {
	handlerConfig, ok := s.handlersConfig[method]
	if ok {
		return handlerConfig.Enabled
	}

	return s.cfg.Default.Enabled
}
