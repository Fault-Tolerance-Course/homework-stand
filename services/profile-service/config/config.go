package config

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"profile-service/internal/pkg/circuit"
	"profile-service/internal/pkg/timeout"

	"github.com/ilyakaznacheev/cleanenv"
)

const (
	AppName = "profile-service"

	AnalyticService = "analytic-service"
)

var (
	instance *Config
	once     sync.Once
)

type Config struct {
	GrpcServer GrpcServer `yaml:"grpc_server"`
	HttpServer HttpServer `yaml:"http_server"`

	Postgres     Postgres       `yaml:"postgres"`
	RedisCluster RedisCluster   `yaml:"redis_cluster"`
	Cache        Cache          `yaml:"cache"`
	Circuit      circuit.Config `yaml:"circuit"`

	ExternalTimeouts timeout.ServiceClientSettings `yaml:"external_timeouts"`

	Graceful Graceful          `yaml:"graceful"`
	Targets  map[string]string `yaml:"service"`
}

func Instance() *Config {
	once.Do(func() {
		instance = &Config{}
		if err := cleanenv.ReadConfig("config/config.yaml", instance); err != nil {
			log.Fatalf("read config error: %s", err.Error())
		}
		if err := cleanenv.ReadEnv(instance); err != nil {
			log.Fatalf("read env error: %s", err.Error())
		}

		if instance.Targets != nil {
			return
		}

		instance.Targets = make(map[string]string)

		for _, env := range os.Environ() {
			if !strings.Contains(env, "_SERVICE_ADDR") {
				continue
			}

			parts := strings.SplitN(env, "=", 2)
			if len(parts) != 2 {
				continue
			}

			serviceName := strings.ToLower(strings.Split(parts[1], ":")[0])
			instance.Targets[serviceName] = parts[1]
		}
	})
	return instance
}

func (c Config) PostgresDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		c.Postgres.User,
		c.Postgres.Password,
		c.Postgres.Host,
		c.Postgres.Port,
		c.Postgres.Database,
	)
}
