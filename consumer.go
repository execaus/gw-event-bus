package gw_kafka_client

import (
	"gw-kafka-client/internal"
	"gw-kafka-client/internal/config"
	"gw-kafka-client/internal/consumer"

	"go.uber.org/zap"
)

type Consumer struct {
	Topics consumer.Topics
}

func NewConsumer(config config.Config, logger *zap.Logger) Consumer {
	internal.Ping(config, logger)

	c := Consumer{
		Topics: consumer.GetTopics(config, logger),
	}
	return c
}
