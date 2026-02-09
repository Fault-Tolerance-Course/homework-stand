package internal

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"syscall"
	"time"

	"task-service/config"
	service "task-service/internal/application/service/task"
	"task-service/internal/infrastructure/gateway"
	"task-service/internal/infrastructure/messagebus"
	"task-service/internal/infrastructure/storage"
	"task-service/internal/pkg/closer"
	"task-service/internal/pkg/healthcheck"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/not-for-prod/clay/server"
	"github.com/not-for-prod/clay/transport"
	"google.golang.org/grpc"
)

// App application
type App struct {
	mainServer *server.Server
	mainMux    *chi.Mux

	adminListener net.Listener
	adminMux      *chi.Mux

	publicCloser *closer.Closer
	adminCloser  *closer.Closer

	controllers []transport.ServiceDesc

	grpcConn map[string]grpc.ClientConnInterface

	pool *pgxpool.Pool

	storages *storage.Registry

	gateways *gateway.Registry

	messageBus *messagebus.Registry

	services *service.Registry

	started    int32
	terminated int32

	// обработчик health check probe
	healthCheck healthcheck.Handler
}

// New конструктор
func New(ctx context.Context) *App {
	app := &App{
		grpcConn:     make(map[string]grpc.ClientConnInterface),
		publicCloser: closer.New(syscall.SIGTERM, syscall.SIGINT),
		adminCloser:  closer.New(),
	}

	// init admin server
	if err := app.initAdminServer(ctx); err != nil {
		log.Fatalf("[APP] Не удалось инициализировать приложение: %s", err.Error())
	}

	// run admin server before init application
	app.runAdminServer(ctx)

	err := app.init(ctx)
	if err != nil {
		log.Fatalf("[APP] Не удалось инициализировать приложение: %s", err.Error())
	}

	return app
}

// Run запуск приложения
func (a *App) Run(_ context.Context) {
	// Сервис запускается в goroutine, и даже если сервис не запустился полностью, то флаг &a.started = 1 уже устанвлен
	if a.mainServer != nil {
		go func() {
			if err := a.mainServer.Run(a.controllers...); err != nil {
				slog.Error(fmt.Sprintf("main server: %s", err.Error()))
				a.publicCloser.CloseAll()
			}
		}()
	}

	slog.Info(fmt.Sprintf("APP STARTED ON PORTS => HTTP: %d, GRPC: %d",
		config.Instance().GrpcServer.Port,
		config.Instance().HttpServer.Port,
	))

	// start signal
	atomic.StoreInt32(&a.started, 1)

	a.publicCloser.Wait()

	closer.CloseAll()
}

// runAdminServer запускает служебный сервер (debug endpoint'ы, healthcheck etc.)
func (a *App) runAdminServer(_ context.Context) {
	adminServer := &http.Server{Handler: a.adminMux}

	go func() {
		if err := adminServer.Serve(a.adminListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.adminCloser.CloseAll()
		}
	}()

	a.adminCloser.Add(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		adminServer.SetKeepAlivesEnabled(false)
		if err := adminServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("error during shutdown admin http server: %w", err)
		}
		return nil
	})
}

func (a *App) init(ctx context.Context) error {
	initFuncs := []func(context.Context) error{
		a.initPostgres,
		a.initGrpcConn,
		a.initMessageBus,
		a.initGateways,
		a.initStorages,
		a.initServices,
		a.initMainServer,
		a.initControllers,
		a.initCategory,
	}

	for _, f := range initFuncs {
		err := f(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
