package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
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
	readMaxBytes = 1 << 20 // 1MB
)

type HandleFunc[MessageT message.Types] = func(message MessageT)

type Topics struct {
	PaymentsHighValueTransfer *Topic[message.PaymentsHighValueTransferMessage]
}

type Topic[MessageT message.Types] struct {
	name       internal.TopicName
	address    string
	partition  int
	logger     *zap.Logger
	handlers   []HandleFunc[MessageT]
	handlersWg sync.WaitGroup
	closeCh    chan struct{}
	conn       *kafka.Conn
}

func (t *Topic[MessageT]) Handle(ctx context.Context, handler HandleFunc[MessageT]) {
	t.handlers = append(t.handlers, handler)

	if t.conn != nil {
		return
	}

	connCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	conn, err := kafka.DialLeader(connCtx, "tcp", t.address, t.name, t.partition)
	if err != nil {
		t.logger.Error(err.Error())
		return
	}

	t.conn = conn
	t.closeCh = make(chan struct{})

	go t.read()
}

func (t *Topic[MessageT]) read() {
	b := make([]byte, readMaxBytes)
	for {
		select {
		case <-t.closeCh:
			return
		default:
		}

		var msg MessageT

		n, err := t.conn.Read(b)
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
			t.handlersWg.Add(1)
			go func() {
				defer t.handlersWg.Done()
				handler(msg)
			}()
		}
	}
}

func (t *Topic[MessageT]) Close() error {
	close(t.closeCh)
	t.handlersWg.Wait()
	return t.conn.Close()
}

func newTopic[MessageT message.Types](name internal.TopicName, partition int, address string, logger *zap.Logger) *Topic[MessageT] {
	t := Topic[MessageT]{
		name:      name,
		address:   address,
		partition: partition,
		logger:    logger,
	}

	return &t
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
