package kafka

import (
	"log"
	"time"

	"task-service/config"

	"github.com/IBM/sarama"
)

func MustConsumerGroup() sarama.ConsumerGroup {
	cfg := sarama.NewConfig()

	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(config.Instance().Kafka.Brokers, config.Instance().Kafka.ConsumerGroup, cfg)
	if err != nil {
		log.Fatalf(err.Error())
		return nil
	}

	return group
}

func MustSyncProducer() sarama.SyncProducer {
	saramaConfig := sarama.NewConfig()

	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Return.Successes = true // для синхронного producer'а

	saramaConfig.Net.DialTimeout = 5 * time.Second
	saramaConfig.Net.WriteTimeout = 5 * time.Second

	saramaConfig.Producer.Retry.Max = 10 // максимальное количество повторов при ошибках
	saramaConfig.Producer.Retry.Backoff = 100 * time.Millisecond
	saramaConfig.Producer.Idempotent = true
	saramaConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	saramaConfig.Producer.Timeout = 10 * time.Second // таймаут на запись
	saramaConfig.Net.MaxOpenRequests = 1             // для идемпотентного продьюсера MaxOpenRequests должен быть 1

	saramaConfig.Metadata.Retry.Max = 5
	saramaConfig.Metadata.Retry.Backoff = 500 * time.Millisecond

	producer, err := sarama.NewSyncProducer(config.Instance().Kafka.Brokers, saramaConfig)
	if err != nil {
		log.Fatalf(err.Error())
		return nil
	}

	return producer
}
