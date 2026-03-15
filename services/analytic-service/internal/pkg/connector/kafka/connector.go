package kafka

import (
	"log"
	"time"

	"analytic-service/config"

	"github.com/IBM/sarama"
)

func MustConsumerGroup() sarama.ConsumerGroup {
	cfg := sarama.NewConfig()

	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	cfg.Consumer.Group.Rebalance.Retry.Max = 4                          // количество повторов при ребалансе
	cfg.Consumer.Group.Rebalance.Retry.Backoff = 500 * time.Millisecond // интервал между попытками

	cfg.Consumer.Group.Session.Timeout = 30 * time.Second   // таймаут сессии (heartbeat)
	cfg.Consumer.Group.Heartbeat.Interval = 5 * time.Second // heartbeat для координации

	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest // для чтения с ошибочного события после ребаланса

	cfg.Consumer.Fetch.Min = 1
	cfg.Consumer.Fetch.Default = 1024 * 1024  // размер батча для чтения
	cfg.Consumer.Fetch.Max = 10 * 1024 * 1024 // максимум за один fetch
	cfg.Consumer.MaxProcessingTime = 500 * time.Millisecond

	group, err := sarama.NewConsumerGroup(config.Instance().Kafka.Brokers, config.Instance().Kafka.ConsumerGroup, cfg)
	if err != nil {
		log.Fatalf(err.Error())
		return nil
	}

	return group
}

// MustRetryConsumerGroup creates a consumer group for the retry topic.
// Uses RetryConsumerGroup ID and longer MaxProcessingTime to support delayed processing.
func MustRetryConsumerGroup() sarama.ConsumerGroup {
	cfg := sarama.NewConfig()

	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	cfg.Consumer.Group.Rebalance.Retry.Max = 4
	cfg.Consumer.Group.Rebalance.Retry.Backoff = 500 * time.Millisecond

	cfg.Consumer.Group.Session.Timeout = 90 * time.Second
	cfg.Consumer.Group.Heartbeat.Interval = 5 * time.Second

	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	cfg.Consumer.Fetch.Min = 1
	cfg.Consumer.Fetch.Default = 1024 * 1024
	cfg.Consumer.Fetch.Max = 10 * 1024 * 1024
	// Must be > retry backoff (30s) so Sarama does not stop fetching
	// while ConsumeClaim waits for the delivery timestamp.
	cfg.Consumer.MaxProcessingTime = 60 * time.Second

	group, err := sarama.NewConsumerGroup(config.Instance().Kafka.Brokers, config.Instance().Kafka.RetryConsumerGroup, cfg)
	if err != nil {
		log.Fatalf(err.Error())
		return nil
	}

	return group
}

// MustSyncProducer creates an idempotent Kafka sync producer.
func MustSyncProducer() sarama.SyncProducer {
	cfg := sarama.NewConfig()

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true

	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1 // required for idempotency

	cfg.Producer.Retry.Max = 10
	cfg.Producer.Retry.Backoff = 100 * time.Millisecond

	cfg.Net.DialTimeout = 5 * time.Second
	cfg.Net.WriteTimeout = 5 * time.Second
	cfg.Producer.Timeout = 10 * time.Second

	cfg.Metadata.Retry.Max = 5
	cfg.Metadata.Retry.Backoff = 500 * time.Millisecond

	producer, err := sarama.NewSyncProducer(config.Instance().Kafka.Brokers, cfg)
	if err != nil {
		log.Fatalf(err.Error())
		return nil
	}

	return producer
}
