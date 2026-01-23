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
	storage        port.EmailsStorage
	notifierClient port.NotifierClient
}

func NewIngestionService(
	storage port.EmailsStorage,
	notifierClient port.NotifierClient,
) *IngestionService {
	return &IngestionService{
		storage:        storage,
		notifierClient: notifierClient,
	}
}

func (i *IngestionService) Run(ctx context.Context, tenantID uuid.UUID) error {
	tenant, err := i.storage.GetTenant(ctx, tenantID)
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
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					log.Warnf("[Worker %d] Context cancelled, stopping ingestion", workerID)
					return
				case user, ok := <-userCh:
					if !ok {
						return
					}
					userCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
					if err := i.ingestMicrosoftUser(userCtx, tenantID, user); err != nil {
						log.Errorf("[Worker %d] Failed to ingest user %s: %v", workerID, user.UserID, err)
						// Do not block if one fails
					}
					cancel()
				}
			}
		}(w)
	}

	// Feed worker pool
	for _, user := range users {
		userCh <- user
	}
	close(userCh)

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
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					log.Warnf("[Worker %d] Context cancelled, stopping ingestion", workerID)
					return
				case user, ok := <-userCh:
					if !ok {
						return
					}

					if err := i.ingestGoogleUser(ctx, tenantID, user); err != nil {
						log.Errorf("[Worker %d] Failed to ingest user %s: %v", workerID, user.UserID, err)
					}
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
	cursor, err := i.storage.LoadCursor(ctx, tenantID, "microsoft", user.UserID)
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

	if len(emails) == 0 {
		return nil
	}

	maxSeen := cursor.LastReceivedAt

	// Create buffered channel for normalized emails
	emailCh := make(chan domain.Email, 10_000)
	errCh := make(chan error, 1)

	// Start batch writer goroutine
	go func() {
		errCh <- i.BatchWriter(ctx, tenantID, user.UserID, emailCh, 500)
	}()

	for _, email := range emails {
		emailCh <- i.NormalizeMicrosoftEmail(tenantID, user.UserID, email)
		if email.ReceivedAt.After(maxSeen) {
			maxSeen = email.ReceivedAt
		}
	}
	close(emailCh)

	// Wait for batch writer to finish
	if err := <-errCh; err != nil {
		return err
	}

	cursor.LastReceivedAt = maxSeen
	cursor.UpdatedAt = time.Now()

	return i.storage.UpsertCursor(ctx, cursor)
}

func (i *IngestionService) ingestGoogleUser(ctx context.Context, tenantID uuid.UUID, user domain.GoogleUser) error {
	cursor, err := i.storage.LoadCursor(ctx, tenantID, "google", user.UserID)
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

	if len(emails) == 0 {
		return nil
	}

	maxSeen := cursor.LastReceivedAt

	emailCh := make(chan domain.Email, 10_000)
	errCh := make(chan error, 1)

	// Start batch writer goroutine
	go func() {
		errCh <- i.BatchWriter(ctx, tenantID, user.UserID, emailCh, 500)
	}()

	for _, email := range emails {
		emailCh <- i.NormalizeGoogleEmail(tenantID, user.UserID, email)
		if email.ReceivedAt.After(maxSeen) {
			maxSeen = email.ReceivedAt
		}
	}
	close(emailCh)

	// Wait for batch writer to finish
	if err := <-errCh; err != nil {
		return err
	}

	cursor.LastReceivedAt = maxSeen
	cursor.UpdatedAt = time.Now()

	return i.storage.UpsertCursor(ctx, cursor)
}

func (i *IngestionService) GetMicrosoftUsers(ctx context.Context, tenantID uuid.UUID) ([]domain.MicrosoftUser, error) {
	return []domain.MicrosoftUser{
		{
			UserID: uuid.New(),
		},
	}, nil
}

func (i *IngestionService) GetMicrosoftEmails(ctx context.Context, userID uuid.UUID, receivedAfter time.Time, orderBy string) ([]domain.MicrosoftEmail, error) {

	// Here it will basically make an API call to Microsoft Graph to fetch emails for the user since receivedAfter.
	// Be sure to use context.WithTimeout not to hang forever.

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

	// Here it will basically make an API call to Google Gmail API to fetch emails for the user since receivedAfter.
	// Be sure to use context.WithTimeout not to hang forever.

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

// BatchWriter processes emails in batches and notifies when each batch is stored.
// Exported for testing purposes.
func (i *IngestionService) BatchWriter(ctx context.Context, tenantID uuid.UUID, userID uuid.UUID, in <-chan domain.Email, batchSize int) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	batch := make([]domain.Email, 0, batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := i.storage.StoreBatch(ctx, batch); err != nil {
			return err
		}
		msg := &domain.NormalizedEmailBatchMessage{
			BatchID:     uuid.New(),
			TenantID:    tenantID,
			UserID:      userID,
			EmailIDList: ExtractEmailIDs(batch),
		}
		if err := i.notifierClient.NotifyEmailBatchIngested(ctx, msg); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
			// continue, don't exit
		case <-ctx.Done():
			_ = flush() // best effort
			return ctx.Err()
		case email, ok := <-in:
			if !ok {
				return flush()
			}
			batch = append(batch, email)
			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

func ExtractEmailIDs(batch []domain.Email) uuid.UUIDs {
	ids := make(uuid.UUIDs, 0, len(batch))
	for _, email := range batch {
		if id, err := uuid.Parse(email.MessageID); err == nil {
			ids = append(ids, id)
		}
	}
	return ids
}
