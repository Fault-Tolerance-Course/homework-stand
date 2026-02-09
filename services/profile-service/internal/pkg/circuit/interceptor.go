package circuit

import (
	"context"
	"errors"
	"log/slog"
	"profile-service/internal/pkg/circuit/metrics"

	"github.com/sony/gobreaker/v2"
	"google.golang.org/grpc"
)

// UnaryClientInterceptor унарный клиентский перехватчик
func (b *Breaker) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		state, ok := b.getBreakerByMethod(method)
		if !ok || state == nil {
			slog.Debug("no breaker found for method or disabled", "method", method)
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		_, err := state.breaker.Execute(func() (interface{}, error) {
			return nil, invoker(ctx, method, req, reply, cc, opts...)
		})

		if err != nil {
			// Если это ошибка circuit breaker, возвращаем ее
			if errors.Is(err, gobreaker.ErrOpenState) {
				metrics.IncRequestCountByState(state.breaker.Name(), metrics.OpenState)
				return ErrCircuitIsOpen
			}

			if errors.Is(err, gobreaker.ErrTooManyRequests) {
				metrics.IncRequestCountByState(state.breaker.Name(), metrics.TooManyRequests)
				return ErrTooManyRequests
			}

			metrics.IncRequestCountByState(state.breaker.Name(), metrics.Other)
			return err
		}

		metrics.IncRequestCountByState(state.breaker.Name(), metrics.NoError)
		return nil
	}
}
