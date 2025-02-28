package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/kelseyhightower/envconfig"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Config struct {
	KafkaBrokers []string `envconfig:"kafka_brokers" default:"localhost:9092"`
	KafkaTopic   string   `envconfig:"kafka_topic" default:"test-topic"`
	GroupID      string   `envconfig:"group_id" default:"test-group"`
}

func main() {
	// Настройка логгера
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	// Загрузка конфигурации из переменных окружения
	var cfg Config
	if err := envconfig.Process("consumer", &cfg); err != nil {
		log.Fatal().Err(err).Msg("Failed to load config")
	}

	// Создание конфигурации Kafka
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Создание consumer group
	group, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, cfg.GroupID, config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create consumer group")
	}
	defer group.Close()

	// Контекст с отменой для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов для graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Consumer instance
	consumer := &Consumer{
		ready: make(chan bool),
	}

	go func() {
		for {
			err := group.Consume(ctx, []string{cfg.KafkaTopic}, consumer)
			if err != nil {
				log.Error().Err(err).Msg("Error from consumer")
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	log.Info().Msg("Consumer is ready")

	// Ожидание сигнала завершения
	<-signals
	log.Info().Msg("Received shutdown signal")
	cancel()
}

// Consumer представляет собой имплементацию sarama.ConsumerGroup
type Consumer struct {
	ready chan bool
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}
			log.Info().
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
