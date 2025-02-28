package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/oke11o/kafka-example/internal/app/consumer"
	consumer_cfg "github.com/oke11o/kafka-example/internal/config/consumer"
	"github.com/oke11o/kafka-example/internal/logger"
)

func main() {
	// Настройка логгера
	log := logger.New()

	// Загрузка конфигурации из переменных окружения
	cfg, err := consumer_cfg.New()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load config")
	}

	// Создание конфигурации для приложения
	appCfg := &consumer.Config{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.GroupID,
	}

	// Создание и запуск консьюмера
	app := consumer.New(appCfg, log)

	// Контекст с отменой для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов для graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signals
		log.Info().Msg("Received shutdown signal")
		cancel()
	}()

	// Запуск приложения
	if err := app.Run(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to run consumer")
	}
	log.Info().Msg("Consumer stopped")
}
