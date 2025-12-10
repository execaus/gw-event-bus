package producer

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
	sendTimeout    = 5 * time.Second
	connectTimeout = 10 * time.Second
)

type Topics struct {
	PaymentsHighValueTransfer Topic[message.PaymentsHighValueTransferMessage]
}

type Topic[MessageT message.Types] struct {
	name      internal.TopicName
	address   string
	partition int
	conn      *kafka.Conn
	logger    *zap.Logger
}

func (t *Topic[MessageT]) Send(ctx context.Context, message MessageT) error {
	if t.conn == nil {
		connCtx, cancel := context.WithTimeout(ctx, connectTimeout)
		defer cancel()

		conn, err := kafka.DialLeader(connCtx, "tcp", t.address, t.name, t.partition)
		if err != nil {
			t.logger.Error(err.Error())
			return err
		}

		t.conn = conn
	}

	data, err := json.Marshal(message)
	if err != nil {
		t.logger.Error(err.Error())
		return err
	}

	if err = t.conn.SetWriteDeadline(time.Now().Add(sendTimeout)); err != nil {
		t.logger.Error(err.Error())
		return err
	}

	_, err = t.conn.Write(data)
	if err != nil {
		t.logger.Error(err.Error())
		return err
	}

	t.logger.Debug("sending message to Kafka topic",
		zap.String("topic", t.name),
		zap.String("data", string(data)),
	)

	return nil
}

func newTopic[MessageT message.Types](name internal.TopicName, partition int, address string, logger *zap.Logger) Topic[MessageT] {
	t := Topic[MessageT]{
		name:      name,
		address:   address,
		partition: partition,
		conn:      nil,
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
