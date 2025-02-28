package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/oke11o/kafka-consumer/internal/app/consumer"
	consumer_cfg "github.com/oke11o/kafka-consumer/internal/config/consumer"
)

func main() {
	// Настройка логгера
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Загрузка конфигурации из переменных окружения
	cfg, err := consumer_cfg.New()
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load config")
	}

	// Создание конфигурации для приложения
	appCfg := &consumer.Config{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.GroupID,
	}

	// Создание и запуск консьюмера
	app := consumer.New(appCfg, logger)

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
		logger.Fatal().Err(err).Msg("Failed to run consumer")
	}
}
