package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	producer_cfg "github.com/oke11o/kafka-consumer/internal/config/producer"
)

func main() {
	// Настройка логгера
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	// Загрузка конфигурации
	cfg, err := producer_cfg.New()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load config")
	}

	// Конфигурация Kafka
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Создание producer
	producer, err := sarama.NewSyncProducer(cfg.KafkaBrokers, config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create producer")
	}
	defer producer.Close()

	// Контекст с отменой для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов для graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Канал для отслеживания завершения отправки сообщений
	done := make(chan bool)

	go func() {
		for i := 1; i <= 10; i++ {
			select {
			case <-ctx.Done():
				done <- true
				return
			default:
				msg := fmt.Sprintf("Message %d at %v", i, time.Now().Format(time.RFC3339))

				partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
					Topic: cfg.KafkaTopic,
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
		done <- true
	}()

	// Ожидание завершения или сигнала
	select {
	case <-signals:
		log.Info().Msg("Received shutdown signal")
		cancel()
	case <-done:
		log.Info().Msg("Finished sending messages")
	}
}
