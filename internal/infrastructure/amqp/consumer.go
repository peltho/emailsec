package amqp

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

// MessageHandler is a function that handles incoming messages
type MessageHandler interface {
	Handle(ctx context.Context, delivery *amqp.Delivery)
}

// Consumer consumes messages from RabbitMQ
type Consumer struct {
	client  *Client
	handler MessageHandler
}

// NewConsumer creates a new consumer
func NewConsumer(client *Client, handler MessageHandler) *Consumer {
	return &Consumer{
		client:  client,
		handler: handler,
	}
}

// Consume starts consuming messages from a queue
func (c *Consumer) Consume(ctx context.Context, queueName string) error {
	ch := c.client.Channel()

	// Set QoS - only process one message at a time
	err := ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	msgs, err := ch.Consume(
		queueName,
		"",    // consumer tag (auto-generated)
		false, // auto-ack (we'll manually ack)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	log.WithField("queue", queueName).Info("Started consuming messages")

	// Process messages
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info("Consumer stopped due to context cancellation")
				return
			case msg, ok := <-msgs:
				if !ok {
					log.Warn("Message channel closed")
					return
				}
				c.handleMessage(ctx, &msg)
			}
		}
	}()

	return nil
}

// handleMessage processes a single message
func (c *Consumer) handleMessage(ctx context.Context, delivery *amqp.Delivery) {
	log.WithFields(log.Fields{
		"routingKey": delivery.RoutingKey,
		"messageId":  delivery.MessageId,
	}).Debug("Processing message")

	// Delegate to the handler
	c.handler.Handle(ctx, delivery)
}
