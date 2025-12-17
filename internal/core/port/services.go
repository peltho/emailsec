package port

import (
	"context"
	"time"

	"github.com/google/uuid"
	"stoik.com/emailsec/internal/core/domain"
)

type IngestionService interface {
	Run(ctx context.Context, tenantID uuid.UUID) error
	GetMicrosoftUsers(ctx context.Context, tenantID uuid.UUID) ([]domain.MicrosoftUser, error)
	GetMicrosoftEmails(ctx context.Context, userID uuid.UUID, receivedAfter time.Time, orderBy string) ([]domain.MicrosoftEmail, error)
	GetGoogleUsers(ctx context.Context, tenantID uuid.UUID) ([]domain.GoogleUser, error)
	GetGoogleEmails(ctx context.Context, userID uuid.UUID, receivedAfter time.Time, orderBy string) ([]domain.GoogleEmail, error)
	NormalizeMicrosoftEmail(tenantID uuid.UUID, userID uuid.UUID, email domain.MicrosoftEmail) domain.Email
	NormalizeGoogleEmail(tenantID uuid.UUID, userID uuid.UUID, email domain.GoogleEmail) domain.Email
}

type FraudDetectionService interface {
	AnalyzeEmail(ctx context.Context, email domain.Email) (bool, error)
}

type EmailStorage interface {
	StoreEmail(ctx context.Context, email domain.Email) error
	GetEmail(ctx context.Context, emailID uuid.UUID) (*domain.Email, error)
}
