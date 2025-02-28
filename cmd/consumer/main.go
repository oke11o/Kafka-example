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
	log := logger.New()
	cfg, err := consumer_cfg.New()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load config")
	}

	app := consumer.New(cfg, log)

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
		log.Fatal().Err(err).Msg("Failed to run consumer")
	}
	log.Info().Msg("Consumer stopped")
}
