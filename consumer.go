package gw_event_bus

import (
	"sync"

	"github.com/execaus/gw-event-bus/internal"
	"github.com/execaus/gw-event-bus/internal/consumer"
	"github.com/hashicorp/go-multierror"

	"go.uber.org/zap"
)

type Closable interface {
	Close() error
}

type Consumer struct {
	Topics consumer.Topics
}

func NewConsumer(host, port string, logger *zap.Logger) Consumer {
	internal.Ping(host, port, logger)

	c := Consumer{
		Topics: consumer.GetTopics(host, port, logger),
	}
	return c
}

func (c *Consumer) Close() error {
	var result *multierror.Error
	var wg sync.WaitGroup

	wg.Add(1)

	go c.closeTopic(c.Topics.PaymentsHighValueTransfer, &wg, result)

	wg.Wait()

	return result.ErrorOrNil()
}

func (c *Consumer) closeTopic(topic Closable, wg *sync.WaitGroup, result *multierror.Error) {
	if err := topic.Close(); err != nil {
		result = multierror.Append(result, err)
	}
	wg.Done()
}
