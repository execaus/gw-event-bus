package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/execaus/gw-event-bus/internal"
	"github.com/execaus/gw-event-bus/message"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
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
	handlers  []HandleFunc[MessageT]
}

func (t *Topic[MessageT]) Handle(ctx context.Context, handler HandleFunc[MessageT]) {
	t.handlers = append(t.handlers, handler)

	if t.batch != nil {
		return
	}

	connCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	conn, err := kafka.DialLeader(connCtx, "tcp", t.address, t.name, t.partition)
	if err != nil {
		t.logger.Error(err.Error())
		return
	}

	t.batch = conn.ReadBatch(batchMinBytes, batchMaxBytes)

	go t.read()
}

func (t *Topic[MessageT]) read() {
	b := make([]byte, batchMinBytes)
	for {
		var msg MessageT

		n, err := t.batch.Read(b)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
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

		for _, handler := range t.handlers {
			handler(msg)
		}
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

func GetTopics(host, port string, logger *zap.Logger) Topics {
	topics := Topics{
		PaymentsHighValueTransfer: newTopic[message.PaymentsHighValueTransferMessage](
			internal.PaymentsHighValueTransferTopicV1,
			0,
			fmt.Sprintf("%s:%s", host, port),
			logger,
		),
	}

	return topics
}
