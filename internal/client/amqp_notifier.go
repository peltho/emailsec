package client

import (
	"context"

	"stoik.com/emailsec/internal/core/domain"
	"stoik.com/emailsec/internal/infrastructure/amqp"
)

type AMQPNotifier struct {
	publisher amqp.Publisher
}

func NewAMQPNotifier(publisher amqp.Publisher) *AMQPNotifier {
	return &AMQPNotifier{
		publisher: publisher,
	}
}

func (n *AMQPNotifier) NotifySuspectingFraudulentEmail(ctx context.Context, message *domain.SuspectingFraudulentEmailMessage) error {
	return n.publisher.Publish(ctx, domain.EmailExchange, domain.RoutingKeyFraudulentDetected, message)
}

func (n *AMQPNotifier) NotifyEmailBatchIngested(ctx context.Context, message *domain.NormalizedEmailBatchMessage) error {
	return n.publisher.Publish(ctx, domain.EmailExchange, domain.RoutingKeyEmailBatchIngested, message)
}
