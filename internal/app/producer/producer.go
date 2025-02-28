package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
)

type Producer struct {
	cfg *Config
}

type Config struct {
	Brokers []string
	Topic   string
}

func New(cfg *Config) *Producer {
	return &Producer{
		cfg: cfg,
	}
}

func (p *Producer) Run(ctx context.Context) error {
	// Конфигурация Kafka
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Создание producer
	producer, err := sarama.NewSyncProducer(p.cfg.Brokers, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer producer.Close()

	// Канал для отслеживания завершения отправки сообщений
	done := make(chan error)

	go func() {
		for i := 1; i <= 10; i++ {
			select {
			case <-ctx.Done():
				done <- nil
				return
			default:
				msg := fmt.Sprintf("Message %d at %v", i, time.Now().Format(time.RFC3339))

				partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
					Topic: p.cfg.Topic,
					Value: sarama.StringEncoder(msg),
				})

				if err != nil {
					log.Error().Err(err).Msg("Failed to send message")
					continue
				}

				log.Info().
					Str("message", msg).
					Int32("partition", partition).
					Int64("offset", offset).
					Msg("Message sent")

				time.Sleep(time.Second)
			}
		}
		done <- nil
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}
