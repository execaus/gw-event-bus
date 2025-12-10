package gw_kafka_client

import (
	"gw-event-bus/internal"
	"gw-event-bus/internal/config"
	"gw-event-bus/internal/producer"

	"go.uber.org/zap"
)

type Producer struct {
	Topics producer.Topics
}

func NewProducer(config config.Config, logger *zap.Logger) Producer {
	internal.Ping(config, logger)

	p := Producer{
		Topics: producer.GetTopics(config, logger),
	}

	return p
}
