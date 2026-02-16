package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"

	"task-service/config"
	v1 "task-service/internal/app/task/v1"
	service "task-service/internal/application/service/task"
	"task-service/internal/infrastructure/gateway"
	"task-service/internal/infrastructure/messagebus"
	"task-service/internal/infrastructure/storage"
	"task-service/internal/pkg/closer"
	"task-service/internal/pkg/connector/postgres"
	"task-service/internal/pkg/grpc/intercept"
	"task-service/internal/pkg/healthcheck"
	taskV1 "task-service/internal/pkg/pb/task-service/task/v1"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/not-for-prod/clay/server"
	"github.com/not-for-prod/clay/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func (a *App) initControllers(_ context.Context) error {
	a.controllers = []transport.ServiceDesc{
		taskV1.NewTaskServiceServiceDesc(v1.NewTaskService(a.services)),
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

func (a *App) initMessageBus(_ context.Context) error {
	if a.messageBus == nil {
		a.messageBus = messagebus.NewRegistry()
	}
	return nil
}

func (a *App) initStorages(ctx context.Context) error {
	if a.storages == nil {
		a.storages = storage.NewRegistry(a.pool)
	}

	go func() {
		err := a.storages.Category.LoadCategories(ctx, config.Instance().Categories.FilePath)
		if err != nil {
			slog.Error(fmt.Sprintf("error while loading categories: %s", err.Error()))
		}
	}()

	// TODO: тут я хочу убедиться,что данные по категориям успешно инициализированы.
	//  После этого приложение может стартовать
	// Но какому типу пробы подходит эта задача?
	return nil
}

func (a *App) initServices(_ context.Context) error {
	if a.services == nil {
		a.services = service.NewRegistry(a.storages, a.gateways, a.messageBus)
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
	a.adminMux = chi.NewMux()

	// init healthcheck for admin server
	if err = a.initHealthCheck(ctx); err != nil {
		return fmt.Errorf("failed to init healthcheck: %w", err)
	}

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
			slog.Warn("task-service: main server gracefully stopped")
		case <-gracefulCtx.Done():
			err := fmt.Errorf("task-service: error while graceful shutdown server: %w", gracefulCtx.Err())
			_ = a.mainServer.Stop(ctx) // TODO: поправить в либе на hard shutdown (да, заметил поздно :) )
			return fmt.Errorf("task-service: stopped: %w", err)
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

	a.healthCheck.AddReadinessCheck("categories", func() error {
		if a.storages.Category.IsLoaded() {
			return nil
		}
		return fmt.Errorf("categories not loaded yet")
	})

	a.adminMux.Post("/cordon", func(writer http.ResponseWriter, request *http.Request) {
		// TODO: как я могу вывести мой под из балансировки тут? Что делать?
		atomic.StoreInt32(&a.terminated, 1)
	})
	a.adminMux.Post("/uncordon", func(writer http.ResponseWriter, request *http.Request) {
		// TODO: как я могу ввести мой под в балансировку тут? Что делать?
		atomic.StoreInt32(&a.terminated, 0)
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
	for _, srv := range []string{config.ProfileService} {
		var err error

		conn, err := grpc.NewClient(config.Instance().Targets[srv],
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithChainUnaryInterceptor(
				intercept.SetClientNameInterceptor(config.AppName),
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
