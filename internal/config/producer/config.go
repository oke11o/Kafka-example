package producer

import "github.com/kelseyhightower/envconfig"

type Config struct {
	KafkaBrokers []string `envconfig:"kafka_brokers" default:"localhost:9092"`
	KafkaTopic   string   `envconfig:"kafka_topic" default:"test-topic"`
}

func New() (*Config, error) {
	var cfg Config
	err := envconfig.Process("producer", &cfg)
	return &cfg, err
}
