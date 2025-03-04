package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"

	producer_cfg "github.com/oke11o/kafka-example/internal/config/producer"
	"github.com/satori/go.uuid"
)

type Producer struct {
	cfg    *producer_cfg.Config
	logger zerolog.Logger
}

// New creates new Producer instance
func New(cfg *producer_cfg.Config, logger zerolog.Logger) *Producer {
	return &Producer{
		cfg:    cfg,
		logger: logger,
	}
}

func (p *Producer) RunSimple(ctx context.Context) error {
	// Конфигурация Kafka
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Создание producer
	producer, err := sarama.NewSyncProducer(p.cfg.KafkaBrokers, config)
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
					Topic: p.cfg.KafkaTopic,
					Value: sarama.StringEncoder(msg),
				})

				if err != nil {
					p.logger.Error().Err(err).Msg("Failed to send message")
					continue
				}

				p.logger.Info().
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

func (p *Producer) Run(ctx context.Context) error {
	// Конфигурация Kafka
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true // Обя зательно для SyncProducer
	config.Producer.Return.Errors = true
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Net.MaxOpenRequests = 1
	config.Producer.Transaction.ID = fmt.Sprintf("txn-%s", uuid.NewV4().String())
	config.Version = sarama.V3_6_0_0

	// Создание producer
	producer, err := sarama.NewSyncProducer(p.cfg.KafkaBrokers, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer producer.Close()

	// Канал для отслеживания завершения отправки сообщений
	done := make(chan error)

	go func() {
		err = producer.BeginTxn()
		if err != nil {
			done <- fmt.Errorf("failed to begin transaction: %w", err)
		}
		msgs := make([]*sarama.ProducerMessage, 0)
		for i := 1; i <= 10; i++ {
			msg := fmt.Sprintf("Message %d at %v", i, time.Now().Format(time.RFC3339))
			msgs = append(msgs, &sarama.ProducerMessage{
				Topic: p.cfg.KafkaTopic,
				Value: sarama.StringEncoder(msg),
			})
		}
		err = producer.SendMessages(msgs)
		if err != nil {
			_ = producer.AbortTxn()
			done <- fmt.Errorf("failed to begin transaction: %w", err)
			return
		}

		if err := producer.CommitTxn(); err != nil {
			_ = producer.AbortTxn()
			done <- fmt.Errorf("failed to begin transaction: %w", err)
			return
		}
		p.logger.Info().Msg("Message committed")
		done <- nil
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}
