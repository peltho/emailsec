package handler

import (
	"context"
	"encoding/json"

	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"

	"stoik.com/emailsec/internal/core/domain"
	"stoik.com/emailsec/internal/core/port"
)

type AMQPConsumer struct {
	fraudDetectionService port.FraudDetectionService
	emailStorage          port.EmailStorage
	validate              *validator.Validate
}

func NewAMQPConsumer(
	fraudDetectionService port.FraudDetectionService,
	emailStorage port.EmailStorage,
	validate *validator.Validate,
) *AMQPConsumer {
	return &AMQPConsumer{
		fraudDetectionService: fraudDetectionService,
		emailStorage:          emailStorage,
		validate:              validate,
	}
}

func (c *AMQPConsumer) Handle(ctx context.Context, delivery *amqp.Delivery) {
	var err error

	switch delivery.RoutingKey {
	case domain.RoutingKeyEmailIngested:
		err = c.handleAnalyzeEmailMessage(ctx, delivery)
	default:
		log.Errorf("unsupported routing key %s", delivery.RoutingKey)
	}

	if err != nil {
		delivery.Nack(false, false)
		return
	}
	delivery.Ack(false)
}

func (c *AMQPConsumer) handleAnalyzeEmailMessage(ctx context.Context, delivery *amqp.Delivery) error {
	var emailEvent domain.NormalizedEmailsBatchMessage

	// Unmarshal the message
	if err := json.Unmarshal(delivery.Body, &emailEvent); err != nil {
		log.Errorf("failed to unmarshal email event: %v", err)
		return err
	}

	// Validate the message
	if err := c.validate.Struct(emailEvent); err != nil {
		log.Errorf("email event validation failed: %v", err)
		return err
	}

	log.WithFields(log.Fields{
		"tenantID": emailEvent.TenantID,
		"batchID":  emailEvent.BatchID,
	}).Info("Processing email batch for fraud detection")

	/*_, err := c.fraudDetectionService.AnalyzeEmail(ctx, emailEvent.Email) // TODO
	if err != nil {
		return err
	}*/

	return nil
}
