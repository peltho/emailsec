package port

import (
	"context"

	"github.com/google/uuid"
	"stoik.com/emailsec/internal/core/domain"
)

type EmailsStorage interface {
	GetTenant(ctx context.Context, tenantID uuid.UUID) (*domain.Tenant, error)
	StoreBatch(ctx context.Context, batch []domain.Email) error
	LoadCursor(ctx context.Context, tenantID uuid.UUID, provider string, userID uuid.UUID) (*domain.IngestionCursor, error)
	UpsertCursor(ctx context.Context, cursor *domain.IngestionCursor) error
	GetEmailsFromBatch(ctx context.Context, emailIDs []uuid.UUID) (map[uuid.UUID]*domain.Email, error)
}
