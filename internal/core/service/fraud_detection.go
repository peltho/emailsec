package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"stoik.com/emailsec/internal/core/domain"
	"stoik.com/emailsec/internal/core/port"
)

type FraudDetectionService struct {
	emailStorage port.EmailsStorage
}

func NewFraudDetectionService(
	emailStorage port.EmailsStorage,
) *FraudDetectionService {
	return &FraudDetectionService{
		emailStorage: emailStorage,
	}
}

type emailJob struct {
	emailID  uuid.UUID
	tenantID uuid.UUID
	userID   uuid.UUID
}

type jobResult struct {
	emailID uuid.UUID
	err     error
}

func (f *FraudDetectionService) Run(ctx context.Context, batchMessage domain.NormalizedEmailBatchMessage) error {
	log.WithFields(log.Fields{
		"tenantID":   batchMessage.TenantID,
		"batchID":    batchMessage.BatchID,
		"userID":     batchMessage.UserID,
		"emailCount": len(batchMessage.EmailIDList),
	}).Info("Starting fraud detection for batch")

	emails, err := f.emailStorage.GetEmailsFromBatch(ctx, batchMessage.EmailIDList)
	if err != nil {
		log.Errorf("failed to fetch emails from batch: %v", err)
		return fmt.Errorf("failed to fetch emails from batch: %w", err)
	}

	// Create a worker pool to process emails concurrently
	processedCount, failedCount := f.processConcurrently(ctx, batchMessage, emails)

	log.WithFields(log.Fields{
		"tenantID":       batchMessage.TenantID,
		"batchID":        batchMessage.BatchID,
		"userID":         batchMessage.UserID,
		"processedCount": processedCount,
		"failedCount":    failedCount,
		"totalCount":     len(batchMessage.EmailIDList),
	}).Info("Completed fraud detection for batch")

	// Return error if all emails failed
	if failedCount > 0 && processedCount == 0 {
		return fmt.Errorf("failed to process all emails in batch")
	}

	return nil
}

func (f *FraudDetectionService) processConcurrently(
	ctx context.Context,
	batchMessage domain.NormalizedEmailBatchMessage,
	emails map[uuid.UUID]*domain.Email,
) (processedCount, failedCount int) {
	jobs := make(chan emailJob, len(batchMessage.EmailIDList))
	results := make(chan jobResult, len(batchMessage.EmailIDList))

	numWorkers := 10 // To adapt
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobs {
				err := f.processEmailFromBatch(ctx, job.emailID, emails)
				results <- jobResult{
					emailID: job.emailID,
					err:     err,
				}
			}
		}()
	}

	// Send jobs to workers
	for _, emailID := range batchMessage.EmailIDList {
		jobs <- emailJob{
			emailID:  emailID,
			tenantID: batchMessage.TenantID,
			userID:   batchMessage.UserID,
		}
	}
	close(jobs)

	// wait for workers to finish
	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		if result.err != nil {
			log.WithFields(log.Fields{
				"emailID":  result.emailID,
				"tenantID": batchMessage.TenantID,
				"userID":   batchMessage.UserID,
				"error":    result.err,
			}).Error("Failed to analyze email")
			failedCount++
		} else {
			processedCount++
		}
	}

	return processedCount, failedCount
}

func (f *FraudDetectionService) processEmailFromBatch(
	ctx context.Context,
	emailID uuid.UUID,
	emails map[uuid.UUID]*domain.Email,
) error {
	// Get email from in-memory batch
	email, exists := emails[emailID]
	if !exists {
		return fmt.Errorf("email not found in batch: %s", emailID)
	}
	if email == nil {
		return fmt.Errorf("email is nil for ID: %s", emailID)
	}

	// TODO
	isFraudulent, err := f.AnalyzeEmail(ctx, *email)
	if err != nil {
		return fmt.Errorf("failed to analyze email: %w", err)
	}

	if isFraudulent {
		log.WithFields(log.Fields{
			"emailID": emailID,
			"from":    email.From,
			"subject": email.Subject,
		}).Warn("Fraudulent email detected")
	}

	// TODO: store the flag back on the email?
	// if so, is it ok to make a DB call for each email (if not a lot of frauds, I suppose..) and notify
	// too many frauds: batch update like ingestion does
	// or: publish a message per fraudulent email in another queue and consume them elsewhere (another consumer for instance)
	// then beware of DB concurrent access to email records! Otherwise just store treated emails in a separate table (processed_emails)

	return nil
}

func (f *FraudDetectionService) AnalyzeEmail(ctx context.Context, email domain.Email) (bool, error) {
	// TODO: Implement actual fraud detection logic
	return false, nil
}
