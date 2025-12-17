package service

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"stoik.com/emailsec/internal/core/domain"
	"stoik.com/emailsec/internal/core/port"
)

type IngestionService struct {
	ingestionStorage port.IngestionStorage
	notifierClient   port.NotifierClient
}

func NewIngestionService(
	ingestionStorage port.IngestionStorage,
	notifierClient port.NotifierClient,
) *IngestionService {
	return &IngestionService{
		ingestionStorage: ingestionStorage,
		notifierClient:   notifierClient,
	}
}

func (i *IngestionService) Run(ctx context.Context, tenantID uuid.UUID) error {
	tenant, err := i.ingestionStorage.GetTenant(ctx, tenantID)
	if err != nil {
		return err
	}

	switch tenant.Provider {
	case "microsoft":
		return i.ingestMicrosoftTenant(ctx, tenantID)
	case "google":
		return i.ingestGoogleTenant(ctx, tenantID)
	}

	return nil
}

func (i *IngestionService) ingestMicrosoftTenant(ctx context.Context, tenantID uuid.UUID) error {
	users, err := i.GetMicrosoftUsers(ctx, tenantID)
	if err != nil {
		return err
	}

	// Worker pool to process users in parallel
	numWorkers := 10 // To adapt based on API rate limits
	userCh := make(chan domain.MicrosoftUser, len(users))

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for user := range userCh {
				if err := i.ingestMicrosoftUser(ctx, tenantID, user); err != nil {
					log.Errorf("[Worker %d] Failed to ingest user %s: %v", workerID, user.UserID, err)
					// Do not block if one fails
				}
			}
		}(w)
	}

	// Feed worker pool
	for _, user := range users {
		userCh <- user
	}
	close(userCh)

	// Wait for all workers to finish
	wg.Wait()

	return nil
}

func (i *IngestionService) ingestGoogleTenant(ctx context.Context, tenantID uuid.UUID) error {
	users, err := i.GetGoogleUsers(ctx, tenantID)
	if err != nil {
		return err
	}

	numWorkers := 10
	userCh := make(chan domain.GoogleUser, len(users))

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for user := range userCh {
				if err := i.ingestGoogleUser(ctx, tenantID, user); err != nil {
					log.Errorf("[Worker %d] Failed to ingest user %s: %v", workerID, user.UserID, err)
				}
			}
		}(w)
	}

	for _, user := range users {
		userCh <- user
	}
	close(userCh)

	wg.Wait()

	return nil
}

func (i *IngestionService) ingestMicrosoftUser(ctx context.Context, tenantID uuid.UUID, user domain.MicrosoftUser) error {
	cursor, err := i.ingestionStorage.LoadCursor(ctx, tenantID, "microsoft", user.UserID)
	if err != nil {
		return err
	}
	receivedAfter := cursor.LastReceivedAt.Add(-2 * time.Minute) // Go back 2min in time

	emails, err := i.GetMicrosoftEmails(
		ctx,
		user.UserID,
		receivedAfter,
		"receivedAt ASC",
	)
	if err != nil {
		return err
	}
	maxSeen := cursor.LastReceivedAt

	// Create buffered channel for normalized emails
	emailCh := make(chan domain.Email, 10_000)
	errCh := make(chan error, 1)

	// Start batch writer goroutine
	go func() {
		errCh <- i.batchWriter(ctx, user.UserID, emailCh, 500)
	}()

	for _, email := range emails {
		normalized := i.NormalizeMicrosoftEmail(tenantID, user.UserID, email)
		emailCh <- normalized

		if email.ReceivedAt.After(maxSeen) {
			maxSeen = email.ReceivedAt
		}
	}

	// Close channel to signal batch writer to finish
	close(emailCh)

	if err := <-errCh; err != nil {
		return err
	}

	cursor.LastReceivedAt = maxSeen
	cursor.UpdatedAt = time.Now()

	return i.ingestionStorage.UpsertCursor(ctx, cursor)
}

func (i *IngestionService) ingestGoogleUser(ctx context.Context, tenantID uuid.UUID, user domain.GoogleUser) error {
	cursor, err := i.ingestionStorage.LoadCursor(ctx, tenantID, "google", user.UserID)
	if err != nil {
		return err
	}
	receivedAfter := cursor.LastReceivedAt.Add(-2 * time.Minute) // Go back 2min ahead in time

	emails, err := i.GetGoogleEmails(
		ctx,
		user.UserID,
		receivedAfter,
		"receivedAt ASC",
	)
	if err != nil {
		return err
	}
	maxSeen := cursor.LastReceivedAt

	emailCh := make(chan domain.Email, 10_000)
	errCh := make(chan error, 1)

	go func() {
		errCh <- i.batchWriter(ctx, user.UserID, emailCh, 500)
	}()

	for _, email := range emails {
		normalized := i.NormalizeGoogleEmail(tenantID, user.UserID, email)
		emailCh <- normalized

		if email.ReceivedAt.After(maxSeen) {
			maxSeen = email.ReceivedAt
		}
	}

	// Close channel to signal batch writer to finish
	close(emailCh)

	if err := <-errCh; err != nil {
		return err
	}

	cursor.LastReceivedAt = maxSeen
	cursor.UpdatedAt = time.Now()

	return i.ingestionStorage.UpsertCursor(ctx, cursor)
}

func (i *IngestionService) GetMicrosoftUsers(ctx context.Context, tenantID uuid.UUID) ([]domain.MicrosoftUser, error) {
	return []domain.MicrosoftUser{
		{
			UserID: uuid.New(),
		},
	}, nil
}

func (i *IngestionService) GetMicrosoftEmails(ctx context.Context, userID uuid.UUID, receivedAfter time.Time, orderBy string) ([]domain.MicrosoftEmail, error) {
	return nil, nil
}

func (i *IngestionService) GetGoogleUsers(ctx context.Context, tenantID uuid.UUID) ([]domain.GoogleUser, error) {
	return []domain.GoogleUser{
		{
			UserID: uuid.New(),
		},
	}, nil
}

func (i *IngestionService) GetGoogleEmails(ctx context.Context, userID uuid.UUID, receivedAfter time.Time, orderBy string) ([]domain.GoogleEmail, error) {
	return nil, nil
}

func (i *IngestionService) NormalizeMicrosoftEmail(tenantID uuid.UUID, userID uuid.UUID, email domain.MicrosoftEmail) domain.Email {

	// Let's suppose there are other transformations to achieve depending on the API.
	// For now we simplify it this way:

	return domain.Email{
		TenantID:   tenantID,
		UserID:     userID,
		MessageID:  email.EmailID.String(),
		From:       email.From,
		To:         email.To,
		Subject:    email.Subject,
		Body:       email.Body,
		Headers:    email.Headers,
		ReceivedAt: email.ReceivedAt,
		Provider:   "microsoft",
	}
}

func (i *IngestionService) NormalizeGoogleEmail(tenantID uuid.UUID, userID uuid.UUID, email domain.GoogleEmail) domain.Email {

	// Let's suppose there are other transformations to achieve depending on the API.
	// For now we simplify it this way:

	return domain.Email{
		TenantID:   tenantID,
		UserID:     userID,
		MessageID:  email.EmailID.String(),
		From:       email.From,
		To:         email.To,
		Subject:    email.Subject,
		Body:       email.Body,
		Headers:    email.Headers,
		ReceivedAt: email.ReceivedAt,
		Provider:   "google",
	}
}

// Here we batch process emails (chunks of 500) not to overflow DB by calling it too much
func (i *IngestionService) batchWriter(ctx context.Context, userID uuid.UUID, in <-chan domain.Email, batchSize int) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	batch := make([]domain.Email, 0, batchSize)
	var lastErr error

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := i.ingestionStorage.StoreBatch(ctx, batch); err != nil {
			log.Errorf("Failed to persist batch: %s", err)
			lastErr = err
			return
		}

		batchMsg := &domain.NormalizedEmailsBatchMessage{
			BatchID:     uuid.New(),
			TenantID:    batch[0].TenantID,
			EmailIDList: extractEmailIDs(batch),
			UserID:      userID,
		}

		i.notifierClient.NotifyEmailBatchIngested(ctx, batchMsg)

		batch = batch[:0]
	}

	for {
		select {
		case email, ok := <-in:
			if !ok {
				// Channel closed, flush and return
				flush()
				return lastErr
			}
			batch = append(batch, email)
			if len(batch) >= batchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-ctx.Done():
			flush()
			return lastErr
		}
	}
}

func extractEmailIDs(batch []domain.Email) uuid.UUIDs {
	ids := make(uuid.UUIDs, 0, len(batch))
	for _, email := range batch {
		if id, err := uuid.Parse(email.MessageID); err == nil {
			ids = append(ids, id)
		}
	}
	return ids
}
