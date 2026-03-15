package consumer

import (
	"context"
	"log/slog"

	"github.com/IBM/sarama"
)

type MessageHandler func(context.Context, sarama.ConsumerGroupSession, *sarama.ConsumerMessage) error

type TopicConsumer struct {
	topic  string
	group  sarama.ConsumerGroup
	cancel context.CancelFunc
}

func NewTopicConsumer(topic string, group sarama.ConsumerGroup) *TopicConsumer {
	return &TopicConsumer{topic: topic, group: group}
}

func (s *TopicConsumer) Subscribe(ctx context.Context, handler MessageHandler) {
	ctx, s.cancel = context.WithCancel(ctx)
	go func() {
		for {
			if ctx.Err() != nil {
				break
			}
			err := s.group.Consume(ctx, []string{s.topic}, groupSubscriber{
				messageHandler: handler,
				setup: func(session sarama.ConsumerGroupSession) error {
					slog.Info("session setup", "member", session.MemberID())
					return nil
				},
				cleanup: func(session sarama.ConsumerGroupSession) error {
					slog.Info("session cleanup", "member", session.MemberID())
					return nil
				},
			})
			if err != nil {
				slog.Error(err.Error())
			}
		}
	}()
}

func (s *TopicConsumer) Stop() {
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
}

func (s *TopicConsumer) Close() error {
	s.Stop()
	if s.group == nil {
		return nil
	}
	return s.group.Close()
}

func (s *TopicConsumer) Errors() <-chan error {
	return s.group.Errors()
}
