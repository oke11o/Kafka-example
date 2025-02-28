package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/oke11o/kafka-consumer/internal/app/producer"
	producer_cfg "github.com/oke11o/kafka-consumer/internal/config/producer"
)

func main() {
	// Настройка логгера
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Загрузка конфигурации
	cfg, err := producer_cfg.New()
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load config")
	}

	// Создание конфигурации для приложения
	appCfg := &producer.Config{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
	}

	// Создание и запуск продюсера
	app := producer.New(appCfg, logger)

	// Контекст с отменой для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов для graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signals
		logger.Info().Msg("Received shutdown signal")
		cancel()
	}()

	// Запуск приложения
	if err := app.Run(ctx); err != nil {
		logger.Fatal().Err(err).Msg("Failed to run producer")
	}
}
