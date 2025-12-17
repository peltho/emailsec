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
	validate              *validator.Validate
}

func NewAMQPConsumer(
	fraudDetectionService port.FraudDetectionService,
	validate *validator.Validate,
) *AMQPConsumer {
	return &AMQPConsumer{
		fraudDetectionService: fraudDetectionService,
		validate:              validate,
	}
}

func (c *AMQPConsumer) Handle(ctx context.Context, delivery *amqp.Delivery) {
	var err error

	switch delivery.RoutingKey {
	case domain.RoutingKeyEmailBatchIngested:
		err = c.handleEmailBatchIngestedMessage(ctx, delivery)
	default:
		log.Errorf("unsupported routing key %s", delivery.RoutingKey)
	}

	if err != nil {
		delivery.Nack(false, false)
		return
	}
	delivery.Ack(false)
}

func (c *AMQPConsumer) handleEmailBatchIngestedMessage(ctx context.Context, delivery *amqp.Delivery) error {
	var batchMessage domain.NormalizedEmailBatchMessage

	if err := json.Unmarshal(delivery.Body, &batchMessage); err != nil {
		log.Errorf("failed to unmarshal email batch message: %v", err)
		return err
	}

	// Validate the message
	if err := c.validate.Struct(batchMessage); err != nil {
		log.Errorf("email batch message validation failed: %v", err)
		return err
	}

	log.WithFields(log.Fields{
		"tenantID":   batchMessage.TenantID,
		"batchID":    batchMessage.BatchID,
		"userID":     batchMessage.UserID,
		"emailCount": len(batchMessage.EmailIDList),
		"ingestedAt": batchMessage.IngestedAt,
	}).Info("Received email batch for fraud detection")

	go func() {
		if err := c.fraudDetectionService.Run(ctx, batchMessage); err != nil {
			log.WithError(err).WithField("batchID", batchMessage.BatchID).Error("Fraud detection failed")
		}
	}()

	return nil
}
