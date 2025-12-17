package storage

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"stoik.com/emailsec/internal/core/domain"
)

type IngestionStorage struct {
	db *PostgresDB
}

func NewIngestionStorage(db *PostgresDB) *IngestionStorage {
	return &IngestionStorage{
		db: db,
	}
}

func (s *IngestionStorage) GetTenant(ctx context.Context, tenantID uuid.UUID) (*domain.Tenant, error) {
	var tenant domain.Tenant
	err := s.db.QueryRow(ctx,
		"SELECT id, name, provider FROM tenants WHERE id = $1",
		tenantID,
	).Scan(&tenant.TenantID, &tenant.Name, &tenant.Provider)

	if err != nil {
		return nil, err
	}

	return &tenant, nil
}

func (s *IngestionStorage) StoreBatch(ctx context.Context, batch []domain.Email) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	batchInsert := `
		INSERT INTO emails (tenant_id, user_id, message_id, from_address, to_addresses, subject, body, headers, received_at, provider)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (tenant_id, message_id) DO NOTHING
	`

	for _, email := range batch {
		_, err := tx.Exec(ctx, batchInsert,
			email.TenantID,
			email.UserID,
			email.MessageID,
			email.From,
			email.To,
			email.Subject,
			email.Body,
			email.Headers,
			email.ReceivedAt,
			email.Provider,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// LoadCursor retrieves the ingestion cursor for a specific tenant/provider/user combination.
// If no cursor exists, it returns a new cursor with LastReceivedAt set to 30 days ago,
// allowing the first ingestion to fetch recent emails.
func (s *IngestionStorage) LoadCursor(ctx context.Context, tenantID uuid.UUID, provider string, userID uuid.UUID) (*domain.IngestionCursor, error) {
	cursor := &domain.IngestionCursor{
		TenantID: tenantID,
		Provider: provider,
		UserID:   userID,
	}

	err := s.db.QueryRow(ctx,
		`SELECT last_received_at, updated_at
		 FROM ingestion_cursors
		 WHERE tenant_id = $1 AND provider = $2 AND user_id = $3`,
		tenantID,
		provider,
		userID,
	).Scan(&cursor.LastReceivedAt, &cursor.UpdatedAt)

	if err == sql.ErrNoRows {
		// No cursor exists yet - initialize with a default starting point
		// Go back 30 days to fetch recent emails on first ingestion
		cursor.LastReceivedAt = time.Now().Add(-30 * 24 * time.Hour)
		cursor.UpdatedAt = time.Now()
		return cursor, nil
	}

	if err != nil {
		return nil, err
	}

	return cursor, nil
}

func (s *IngestionStorage) UpsertCursor(ctx context.Context, cursor *domain.IngestionCursor) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO ingestion_cursors (tenant_id, provider, user_id, last_received_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (tenant_id, provider, user_id)
		 DO UPDATE SET
		     last_received_at = EXCLUDED.last_received_at,
		     updated_at = EXCLUDED.updated_at`,
		cursor.TenantID,
		cursor.Provider,
		cursor.UserID,
		cursor.LastReceivedAt,
		cursor.UpdatedAt,
	)

	return err
}
