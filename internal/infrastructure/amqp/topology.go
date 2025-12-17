package amqp

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

const (
	EmailExchange = "email"

	EmailAnalysisQueue = "email.analysis"

	RoutingKeyEmailIngested = "email.ingested"
)

// TopologyManager handles the declaration of exchanges, queues, and bindings
type TopologyManager struct {
	client *Client
}

func NewTopologyManager(client *Client) *TopologyManager {
	return &TopologyManager{
		client: client,
	}
}

// Setup declares all exchanges, queues, and bindings
func (t *TopologyManager) Setup() error {
	ch := t.client.Channel()

	// Declare email exchange
	if err := t.declareExchange(ch, EmailExchange); err != nil {
		return err
	}

	// Declare email analysis queue
	if err := t.declareQueue(ch, EmailAnalysisQueue); err != nil {
		return err
	}

	// Bind queue to exchange
	if err := t.bindQueue(ch, EmailAnalysisQueue, EmailExchange, RoutingKeyEmailIngested); err != nil {
		return err
	}

	log.Info("AMQP topology setup completed successfully")
	return nil
}

// declareExchange declares a topic exchange
func (t *TopologyManager) declareExchange(ch *amqp.Channel, name string) error {
	err := ch.ExchangeDeclare(
		name,
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange '%s': %w", name, err)
	}

	log.WithField("exchange", name).Debug("Exchange declared")
	return nil
}

// declareQueue declares a durable queue
func (t *TopologyManager) declareQueue(ch *amqp.Channel, name string) error {
	_, err := ch.QueueDeclare(
		name,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue '%s': %w", name, err)
	}

	log.WithField("queue", name).Debug("Queue declared")
	return nil
}

// bindQueue binds a queue to an exchange with a routing key
func (t *TopologyManager) bindQueue(ch *amqp.Channel, queueName, exchangeName, routingKey string) error {
	err := ch.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue '%s' to exchange '%s' with routing key '%s': %w",
			queueName, exchangeName, routingKey, err)
	}

	log.WithFields(log.Fields{
		"queue":      queueName,
		"exchange":   exchangeName,
		"routingKey": routingKey,
	}).Debug("Queue bound to exchange")
	return nil
}
