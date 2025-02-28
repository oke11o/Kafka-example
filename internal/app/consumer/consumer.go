package consumer

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

type Consumer struct {
	ready  chan bool
	cfg    *Config
	logger zerolog.Logger
}

type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

func New(cfg *Config, logger zerolog.Logger) *Consumer {
	return &Consumer{
		ready:  make(chan bool),
		cfg:    cfg,
		logger: logger,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	// Создание конфигурации Kafka
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Создание consumer group
	group, err := sarama.NewConsumerGroup(c.cfg.Brokers, c.cfg.GroupID, config)
	if err != nil {
		return err
	}
	defer group.Close()

	// Запуск процесса потребления
	go func() {
		for {
			err := group.Consume(ctx, []string{c.cfg.Topic}, c)
			if err != nil {
				c.logger.Error().Err(err).Msg("Error from consumer")
			}
			if ctx.Err() != nil {
				return
			}
			c.ready = make(chan bool)
		}
	}()

	<-c.ready
	c.logger.Info().Msg("Consumer is ready")

	<-ctx.Done()
	return nil
}

// Setup реализует интерфейс sarama.ConsumerGroupHandler
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

// Cleanup реализует интерфейс sarama.ConsumerGroupHandler
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim реализует интерфейс sarama.ConsumerGroupHandler
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}
			c.logger.Info().
				Str("topic", message.Topic).
				Int32("partition", message.Partition).
				Int64("offset", message.Offset).
				Bytes("key", message.Key).
				Bytes("value", message.Value).
				Msg("Received message")

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
