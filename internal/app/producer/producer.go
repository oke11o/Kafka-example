package producer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/satori/go.uuid"

	producer_cfg "github.com/oke11o/kafka-example/internal/config/producer"
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
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(p.cfg.KafkaBrokers, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer producer.Close()

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
	producer, err := NewKafkaProducer(p.cfg.KafkaBrokers, p.logger)
	if err != nil {
		log.Fatalf("Ошибка создания продюсера: %v", err)
	}
	defer producer.Close()

	msgs := make([]*sarama.ProducerMessage, 0)
	for i := 1; i <= 10; i++ {
		msg := fmt.Sprintf("Message %d at %v", i, time.Now().Format(time.RFC3339))
		msgs = append(msgs, &sarama.ProducerMessage{
			Topic: p.cfg.KafkaTopic,
			Value: sarama.StringEncoder(msg),
		})
	}
	err = producer.SendBatchWithRetry(ctx, msgs)
	if err != nil {
		log.Printf("Ошибка отправки batch: %v", err)
		return err
	}
	return nil
}

func getProducerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Net.MaxOpenRequests = 1
	config.Producer.Transaction.ID = fmt.Sprintf("txn-%s", uuid.NewV4().String())
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true // Обязательно для SyncProducer
	config.Version = sarama.V3_6_0_0

	return config
}

type KafkaProducer struct {
	producer sarama.SyncProducer
	brokers  []string
	logger   zerolog.Logger
}

func NewKafkaProducer(brokers []string, logger zerolog.Logger) (*KafkaProducer, error) {
	config := getProducerConfig()

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания продюсера: %w", err)
	}

	return &KafkaProducer{
		producer: producer,
		brokers:  brokers,
		logger:   logger,
	}, nil
}

func (kp *KafkaProducer) SendBatchWithRetry(ctx context.Context, messages []*sarama.ProducerMessage) error {
	const (
		maxRetries = 3
		baseDelay  = time.Second
	)

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := kp.producer.BeginTxn(); err != nil {
			lastErr = fmt.Errorf("ошибка начала транзакции: %w", err)
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		err := kp.producer.SendMessages(messages)
		if err != nil {
			if err := kp.producer.AbortTxn(); err != nil {
				kp.logger.Info().Msgf("Ошибка отмены транзакции: %v", err)
			}
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		if err := kp.producer.CommitTxn(); err != nil {
			lastErr = fmt.Errorf("ошибка коммита транзакции: %w", err)
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	return fmt.Errorf("превышено количество попыток. Последняя ошибка: %w", lastErr)
}

func (kp *KafkaProducer) Close() error {
	return kp.producer.Close()
}
