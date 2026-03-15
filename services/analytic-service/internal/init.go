package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"sync/atomic"

	"analytic-service/config"
	v1 "analytic-service/internal/app/analytic/v1"
	"analytic-service/internal/applicaton/service"
	"analytic-service/internal/infrastructure/messagebus"
	"analytic-service/internal/infrastructure/messagebus/subscriber/scheme/task_events/task_created"
	"analytic-service/internal/infrastructure/storage"
	"analytic-service/internal/pkg/connector/postgres"

	eventrouter "analytic-service/internal/pkg/event-router"

	"analytic-service/internal/pkg/grpc/intercept"
	"analytic-service/internal/pkg/healthcheck"
	analyticV1 "analytic-service/internal/pkg/pb/analytic-service/analytic/v1"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"

	"github.com/not-for-prod/clay/server"
	"github.com/not-for-prod/clay/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func (a *App) initControllers(_ context.Context) error {
	a.controllers = []transport.ServiceDesc{
		analyticV1.NewAnalyticServiceServiceDesc(
			v1.NewAnalyticService(a.services),
		),
	}
	return nil
}

func (a *App) initPostgres(ctx context.Context) error {
	pool, err := postgres.Pool(ctx, config.Instance().PostgresDSN())
	if err != nil {
		return fmt.Errorf("[POSTGRES] Не удалось инициализировать pool: %s", err.Error())
	}

	a.pool = pool
	return nil
}

func (a *App) initStorages(_ context.Context) error {
	if a.storages == nil {
		a.storages = storage.NewRegistry(a.pool)
	}
	return nil
}

func (a *App) initServices(_ context.Context) error {
	if a.services == nil {
		a.services = service.NewRegistry(a.storages)
	}
	return nil
}

func (a *App) initHandlers(_ context.Context) error {
	a.eventRouter = eventrouter.NewEventRouter[string, []byte]()

	a.eventRouter.RegisterAll(task_created.NewMessageHandler(a.services.AcceptTask))

	return nil
}

func (a *App) initMessageBus(_ context.Context) error {
	if a.messageBus == nil {
		a.messageBus = messagebus.NewRegistry(a.eventRouter)
	}
	return nil
}

func (a *App) initAdminServer(ctx context.Context) error {
	// init admin listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", config.Instance().HttpServer.AdminPort))
	if err != nil {
		return fmt.Errorf("failed to init admin listener: %w", err)
	}

	a.adminListener = lis

	// init healthcheck for admin server
	if err = a.initHealthCheck(ctx); err != nil {
		return fmt.Errorf("failed to init healthcheck: %w", err)
	}

	a.adminMux = chi.NewMux()

	a.adminMux.Mount("/debug", chimw.Profiler())

	// register healthcheck
	a.adminMux.HandleFunc(healthcheck.LivenessPath, a.healthCheck.LiveEndpoint)
	a.adminMux.HandleFunc(healthcheck.ReadinessPath, a.healthCheck.ReadyEndpoint)

	return nil
}

func (a *App) initMainServer(ctx context.Context) error {
	a.mainMux = chi.NewMux()

	// init server (htt,grpc)
	a.mainServer = server.NewServer(
		config.Instance().GrpcServer.Port,
		server.WithHTTPMux(a.mainMux),
		server.WithHTTPPort(config.Instance().HttpServer.Port),
		server.WithGRPCOpts(
			grpc.KeepaliveParams(keepalive.ServerParameters{
				MaxConnectionIdle: config.Instance().GrpcServer.MaxConnectionIdle,
				MaxConnectionAge:  config.Instance().GrpcServer.MaxConnectionAge,
				Time:              config.Instance().GrpcServer.Time,
				Timeout:           config.Instance().GrpcServer.Timeout,
			}),
			grpc.ChainUnaryInterceptor(
				intercept.ErrorInterceptor(),
			),
		),
	)

	a.publicCloser.Add(func() error {
		gracefulCtx, cancel := context.WithTimeout(context.Background(), config.Instance().Graceful.Timeout)
		defer cancel()

		done := make(chan struct{})
		go func() {
			err := a.mainServer.Stop(gracefulCtx)
			if err != nil {
				slog.Error(fmt.Sprintf("stop main server error: %s", err.Error()))
			}
			close(done)
		}()

		select {
		case <-done:
			slog.Warn("analytic-service: main server gracefully stopped")
		case <-gracefulCtx.Done():
			err := fmt.Errorf("analytic-service: error while graceful shutdown server: %w", gracefulCtx.Err())
			_ = a.mainServer.Stop(ctx) // TODO: поправить в либе на hard shutdown (да, заметил поздно :) )
			return fmt.Errorf("analytic-gateway: stopped: %w", err)
		}
		return nil
	})

	return nil
}

func (a *App) initHealthCheck(_ context.Context) error {
	a.healthCheck = healthcheck.NewHandler()

	// поверяю, что нет утечки горутин на старте (как пример)
	a.healthCheck.AddLivenessCheck("goroutines", func() error {
		if runtime.NumGoroutine() < 1000 {
			return nil
		}
		return fmt.Errorf("application has too much running goroutines")
	})

	// readiness - т.к. я уже проинициализировал все компоненты
	a.healthCheck.AddReadinessCheck("started", func() error {
		if atomic.LoadInt32(&a.started) != 0 {
			return nil
		}
		return fmt.Errorf("application is not statred yet")
	})

	a.healthCheck.AddReadinessCheck("termination", func() error {
		if atomic.LoadInt32(&a.terminated) == 0 {
			return nil
		}
		return fmt.Errorf("application is terminating now")
	})

	// Почему мне нужен этот флажок?
	// И зачем я его выставляю по завершению работы приложения?
	a.publicCloser.Add(func() error {
		slog.Warn(fmt.Sprintf("app got termination signal, graceful config timeout: %s",
			config.Instance().Graceful.Timeout.String()))

		atomic.StoreInt32(&a.terminated, 1)
		return nil
	})
	return nil
}
