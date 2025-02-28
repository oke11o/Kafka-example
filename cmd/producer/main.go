package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/oke11o/kafka-example/internal/app/producer"
	producer_cfg "github.com/oke11o/kafka-example/internal/config/producer"
	"github.com/oke11o/kafka-example/internal/logger"
)

func main() {
	log := logger.New()
	cfg, err := producer_cfg.New()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load config")
	}

	app := producer.New(cfg, log)

	// Graceful
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		log.Info().Msg("Received shutdown signal")
		cancel()
	}()

	// Run
	if err := app.Run(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to run producer")
	}
	log.Info().Msg("Producer stopped")
}
