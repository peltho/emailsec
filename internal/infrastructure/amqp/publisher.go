package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

type Publisher struct {
	client *Client
}

func NewPublisher(client *Client) *Publisher {
	return &Publisher{
		client: client,
	}
}

// Publish publishes a message to an exchange with a routing key
func (p *Publisher) Publish(ctx context.Context, exchange, routingKey string, message interface{}) error {
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Add timeout to context if not already present
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}

	err = p.client.Channel().PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
		},
	)

	if err != nil {
		return fmt.Errorf("failed to publish message to exchange '%s' with routing key '%s': %w", exchange, routingKey, err)
	}

	log.WithFields(log.Fields{
		"exchange":   exchange,
		"routingKey": routingKey,
	}).Debug("Message published successfully")

	return nil
}

// PublishWithConfirm publishes a message and waits for broker confirmation
func (p *Publisher) PublishWithConfirm(ctx context.Context, exchange, routingKey string, message interface{}) error {
	ch := p.client.Channel()

	// Enable publisher confirms
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("failed to enable publisher confirms: %w", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	// Publish the message
	if err := p.Publish(ctx, exchange, routingKey, message); err != nil {
		return err
	}

	// Wait for confirmation
	select {
	case confirmation := <-confirms:
		if !confirmation.Ack {
			return fmt.Errorf("message was nacked by broker")
		}
		log.Debug("Message confirmed by broker")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("confirmation timeout: %w", ctx.Err())
	}
}
