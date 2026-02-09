package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"sync/atomic"

	"profile-service/config"
	v1 "profile-service/internal/app/profile/v1"
	"profile-service/internal/application/service"
	"profile-service/internal/infrastructure/adapter"
	"profile-service/internal/infrastructure/gateway"
	"profile-service/internal/infrastructure/storage"
	"profile-service/internal/pkg/circuit"
	"profile-service/internal/pkg/closer"
	"profile-service/internal/pkg/connector/postgres"
	"profile-service/internal/pkg/grpc/intercept"
	"profile-service/internal/pkg/healthcheck"
	profileV1 "profile-service/internal/pkg/pb/profile-service/profile/v1"
	"profile-service/internal/pkg/timeout"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/not-for-prod/clay/server"
	"github.com/not-for-prod/clay/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pkgredis "profile-service/internal/pkg/connector/redis"
)

func (a *App) initControllers(_ context.Context) error {
	a.controllers = []transport.ServiceDesc{
		profileV1.NewProfileServiceServiceDesc(v1.NewProfileService(a.services)),
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

func (a *App) initRedis(ctx context.Context) error {
	client, err := pkgredis.NewShardedClient(ctx)
	if err != nil {
		return fmt.Errorf("[REDIS] Не удалось инициализировать клиента: %s", err.Error())
	}

	a.redis = client

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
		a.services = service.NewRegistry(a.storages, a.gateways, a.adapters)
	}
	return nil
}

func (a *App) initAdapter(_ context.Context) error {
	if a.adapters == nil {
		a.adapters = adapter.NewRegistry(a.redis, a.storages)
	}
	return nil
}

func (a *App) initGateways(_ context.Context) error {
	if a.gateways == nil {
		a.gateways = gateway.NewRegistry(a.grpcConn)
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

	a.adminMux.Handle("/metrics", promhttp.Handler())

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
			slog.Warn("profile-service: main server gracefully stopped")
		case <-gracefulCtx.Done():
			err := fmt.Errorf("profile-service: error while graceful shutdown server: %w", gracefulCtx.Err())
			_ = a.mainServer.Stop(ctx) // TODO: поправить в либе на hard shutdown (да, заметил поздно :) )
			return fmt.Errorf("profile-service: stopped: %w", err)
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

func (a *App) initGrpcConn(_ context.Context) error {
	// регистрируем конфиги всех сервисов, для которых у нас есть timeout'ы
	timeout.SetServiceClientTimeoutSettings(config.Instance().ExternalTimeouts)

	for _, srv := range []string{config.AnalyticService} {
		var (
			err    error
			target = config.Instance().Targets[srv]
		)

		conn, err := grpc.NewClient(target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithChainUnaryInterceptor(
				intercept.SetClientNameInterceptor(config.AppName),
				circuit.NewCircuitBreaker(config.Instance().Circuit).UnaryClientInterceptor(),
				timeout.NewClientTimeoutInterceptor(srv),
			),
		)

		if err != nil {
			return fmt.Errorf("не удалось инициализировать grpc соединение к %s : %s", srv, err.Error())
		}

		a.grpcConn[srv] = conn
		closer.Add(conn.Close)
	}
	return nil
}
