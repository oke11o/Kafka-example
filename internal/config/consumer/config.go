package consumer

import "github.com/kelseyhightower/envconfig"

type Config struct {
	KafkaBrokers []string `envconfig:"kafka_brokers" default:"localhost:9092"`
	KafkaTopic   string   `envconfig:"kafka_topic" default:"test-topic"`
	GroupID      string   `envconfig:"group_id" default:"test-group"`
}

func New() (*Config, error) {
	var cfg Config
	err := envconfig.Process("consumer", &cfg)
	return &cfg, err
}
