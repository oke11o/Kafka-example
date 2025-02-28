package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/oke11o/kafka-consumer/internal/app/producer"
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

	// Создание конфигурации для приложения
	appCfg := &producer.Config{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
	}

	// Создание и запуск продюсера
	app := producer.New(appCfg)

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
		log.Fatal().Err(err).Msg("Failed to run producer")
	}
}
