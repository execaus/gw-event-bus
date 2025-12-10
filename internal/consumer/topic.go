package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"gw-event-bus/internal"
	"gw-event-bus/internal/config"
	"gw-event-bus/internal/message"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	readTimeout    = 5 * time.Second
	connectTimeout = 10 * time.Second
)

const (
	batchMinBytes = 10 << 10 // 10KB
	batchMaxBytes = 1 << 20  // 1MB
)

type HandleFunc[MessageT message.Types] = func(message MessageT)

type Topics struct {
	PaymentsHighValueTransfer Topic[message.PaymentsHighValueTransferMessage]
}

type Topic[MessageT message.Types] struct {
	name      internal.TopicName
	address   string
	partition int
	batch     *kafka.Batch
	logger    *zap.Logger
}

func (t *Topic[MessageT]) Handle(ctx context.Context, handler HandleFunc[MessageT]) {
	if t.batch == nil {
		connCtx, cancel := context.WithTimeout(ctx, connectTimeout)
		defer cancel()

		conn, err := kafka.DialLeader(connCtx, "tcp", t.address, t.name, t.partition)
		if err != nil {
			t.logger.Error(err.Error())
			return
		}

		if err = conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			t.logger.Error(err.Error())
			return
		}

		t.batch = conn.ReadBatch(batchMinBytes, batchMaxBytes)
	}

	b := make([]byte, batchMinBytes)
	for {
		var msg MessageT

		n, err := t.batch.Read(b)
		if err != nil {
			t.logger.Error(err.Error())
			continue
		}

		if err = json.Unmarshal(b[:n], &msg); err != nil {
			t.logger.Error(err.Error())
			continue
		}

		t.logger.Debug("received message from Kafka topic",
			zap.String("topic", t.name),
			zap.ByteString("data", b[:n]),
		)

		handler(msg)
	}
}

func newTopic[MessageT message.Types](name internal.TopicName, partition int, address string, logger *zap.Logger) Topic[MessageT] {
	t := Topic[MessageT]{
		name:      name,
		address:   address,
		partition: partition,
		batch:     nil,
		logger:    logger,
	}

	return t
}

func GetTopics(config config.Config, logger *zap.Logger) Topics {
	topics := Topics{
		PaymentsHighValueTransfer: newTopic[message.PaymentsHighValueTransferMessage](
			internal.PaymentsHighValueTransferTopicV1,
			0,
			fmt.Sprintf("%s:%s", config.Host, config.Port),
			logger,
		),
	}

	return topics
}
