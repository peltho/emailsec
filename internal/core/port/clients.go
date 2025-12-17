package port

import (
	"context"

	"stoik.com/emailsec/internal/core/domain"
)

type NotifierClient interface {
	NotifySuspectingFraudulentEmail(ctx context.Context, message *domain.SuspectingFraudulentEmailMessage) error
	NotifyEmailBatchIngested(ctx context.Context, event *domain.NormalizedEmailBatchMessage) error
}
