package client

import (
	"context"

	"stoik.com/emailsec/internal/core/domain"
)

const (
	EmailExchange = "email"
)

type Publisher interface {
	Publish(ctx context.Context, exchange, routingKey string, message any) error
}

type AMQPNotifier struct {
	publisher Publisher
}

func NewAMQPNotifier(publisher Publisher) *AMQPNotifier {
	return &AMQPNotifier{
		publisher: publisher,
	}
}

func (n *AMQPNotifier) NotifySuspectingFraudulentEmail(ctx context.Context, message *domain.SuspectingFraudulentEmailMessage) error {
	return n.publisher.Publish(ctx, EmailExchange, domain.RoutingKeyFraudulentDetected, message)
}

func (n *AMQPNotifier) NotifyEmailBatchIngested(ctx context.Context, message *domain.NormalizedEmailBatchMessage) error {
	return n.publisher.Publish(ctx, EmailExchange, domain.RoutingKeyEmailBatchIngested, message)
}
