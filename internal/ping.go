package internal

import (
	"context"
	"fmt"
	"gw-event-bus/internal/config"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	connectTimeout = 5 * time.Second
)

func Ping(config config.Config, logger *zap.Logger) {
	address := fmt.Sprintf("%s:%s", config.Host, config.Port)

	connCtx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	conn, err := kafka.DialContext(connCtx, "tcp", address)
	if err != nil {
		logger.Fatal("kafka ping failed", zap.String("address", address), zap.Error(err))
	} else {
		brokers, err := conn.Brokers()
		if err != nil {
			logger.Fatal(err.Error())
		}
		logger.Debug("kafka ping successful", zap.String("address", address), zap.Any("brokers", brokers))
		if err = conn.Close(); err != nil {
			logger.Fatal(err.Error())
		}
	}
}
