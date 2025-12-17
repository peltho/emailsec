package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"stoik.com/emailsec/internal/core/domain"
	"stoik.com/emailsec/mocks"
)

type IngestionServiceSuite struct {
	suite.Suite
	ingestionStorage *mocks.IngestionStorage
	ingestionService *IngestionService
	amqpNotifier     *mocks.NotifierClient
}

func TestIngestionService(t *testing.T) {
	suite.Run(t, new(IngestionServiceSuite))
}

func (suite *IngestionServiceSuite) SetupSuite() {
	if suite.T().Failed() {
		suite.T().FailNow()
	}
}

func (suite *IngestionServiceSuite) TearDownTest() {
	suite.ingestionStorage.AssertExpectations(suite.T())
	suite.amqpNotifier.AssertExpectations(suite.T())
}

func (suite *IngestionServiceSuite) SetupTest() {
	suite.ingestionStorage = &mocks.IngestionStorage{}
	suite.amqpNotifier = &mocks.NotifierClient{}
	suite.ingestionService = NewIngestionService(suite.ingestionStorage, suite.amqpNotifier)
}

func (suite *IngestionServiceSuite) TestRun_MicrosoftProvider() {
	ctx := context.Background()
	tenantID := uuid.New()
	userID := uuid.New()
	baseTime := time.Now().Add(-1 * time.Hour)

	tenant := &domain.Tenant{
		TenantID: tenantID,
		Provider: "microsoft",
		Name:     "Test Tenant",
	}

	cursor := &domain.IngestionCursor{
		TenantID:       tenantID,
		Provider:       "microsoft",
		UserID:         userID,
		LastReceivedAt: baseTime,
		UpdatedAt:      baseTime,
	}

	suite.ingestionStorage.EXPECT().GetTenant(ctx, tenantID).Return(tenant, nil)
	suite.ingestionStorage.EXPECT().LoadCursor(ctx, tenantID, "microsoft", mock.Anything).Return(cursor, nil)
	suite.ingestionStorage.EXPECT().UpsertCursor(ctx, mock.Anything).Return(nil)

	// Maybe because it's not mandatory there are emails
	suite.ingestionStorage.EXPECT().StoreBatch(ctx, mock.Anything).Return(nil).Maybe()
	suite.amqpNotifier.EXPECT().NotifyEmailBatchIngested(ctx, mock.Anything).Return(nil).Maybe()

	err := suite.ingestionService.Run(ctx, tenantID)

	assert.NoError(suite.T(), err)
}

func (suite *IngestionServiceSuite) TestRun_GoogleProvider() {
	ctx := context.Background()
	tenantID := uuid.New()
	userID := uuid.New()
	baseTime := time.Now().Add(-1 * time.Hour)

	tenant := &domain.Tenant{
		TenantID: tenantID,
		Provider: "google",
		Name:     "Test Tenant",
	}

	cursor := &domain.IngestionCursor{
		TenantID:       tenantID,
		Provider:       "google",
		UserID:         userID,
		LastReceivedAt: baseTime,
		UpdatedAt:      baseTime,
	}

	suite.ingestionStorage.EXPECT().GetTenant(ctx, tenantID).Return(tenant, nil)
	suite.ingestionStorage.EXPECT().LoadCursor(ctx, tenantID, "google", mock.Anything).Return(cursor, nil)
	suite.ingestionStorage.EXPECT().UpsertCursor(ctx, mock.Anything).Return(nil)

	// Same as above
	suite.ingestionStorage.EXPECT().StoreBatch(ctx, mock.Anything).Return(nil).Maybe()
	suite.amqpNotifier.EXPECT().NotifyEmailBatchIngested(ctx, mock.Anything).Return(nil).Maybe()

	err := suite.ingestionService.Run(ctx, tenantID)

	assert.NoError(suite.T(), err)
}

func (suite *IngestionServiceSuite) TestRun_TenantNotFound() {

	ctx := context.Background()
	tenantID := uuid.New()

	expectedErr := errors.New("tenant not found")
	suite.ingestionStorage.EXPECT().GetTenant(ctx, tenantID).Return(nil, expectedErr)

	err := suite.ingestionService.Run(ctx, tenantID)

	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), expectedErr, err)
}

func (suite *IngestionServiceSuite) TestRun_UnknownProvider() {
	ctx := context.Background()
	tenantID := uuid.New()

	tenant := &domain.Tenant{
		TenantID: tenantID,
		Provider: "unknown",
		Name:     "Test Tenant",
	}

	suite.ingestionStorage.EXPECT().GetTenant(ctx, tenantID).Return(tenant, nil)

	err := suite.ingestionService.Run(ctx, tenantID)

	// Should return nil for unknown provider (based on the switch statement)
	assert.NoError(suite.T(), err)
}

func (suite *IngestionServiceSuite) TestNormalizeMicrosoftEmail() {
	tenantID := uuid.New()
	userID := uuid.New()
	emailID := uuid.New()
	receivedAt := time.Now()

	msEmail := domain.MicrosoftEmail{
		EmailID:        emailID,
		ReceivedAt:     receivedAt,
		HasAttachments: true,
		From:           "sender@example.com",
		To:             []string{"recipient@example.com"},
		Subject:        "Test Subject",
		Body:           "Test Body",
		Headers:        map[string]string{"X-Header": "value"},
	}

	normalized := suite.ingestionService.NormalizeMicrosoftEmail(tenantID, userID, msEmail)

	assert.Equal(suite.T(), tenantID, normalized.TenantID)
	assert.Equal(suite.T(), userID, normalized.UserID)
	assert.Equal(suite.T(), emailID.String(), normalized.MessageID)
	assert.Equal(suite.T(), "sender@example.com", normalized.From)
	assert.Equal(suite.T(), []string{"recipient@example.com"}, normalized.To)
	assert.Equal(suite.T(), "Test Subject", normalized.Subject)
	assert.Equal(suite.T(), "Test Body", normalized.Body)
	assert.Equal(suite.T(), "microsoft", normalized.Provider)
	assert.Equal(suite.T(), receivedAt, normalized.ReceivedAt)
	assert.Equal(suite.T(), map[string]string{"X-Header": "value"}, normalized.Headers)
}

func (suite *IngestionServiceSuite) TestNormalizeGoogleEmail() {
	tenantID := uuid.New()
	userID := uuid.New()
	emailID := uuid.New()
	receivedAt := time.Now()

	googleEmail := domain.GoogleEmail{
		EmailID:        emailID,
		ReceivedAt:     receivedAt,
		HasAttachments: false,
		From:           "sender@gmail.com",
		To:             []string{"recipient@gmail.com"},
		Subject:        "Google Subject",
		Body:           "Google Body",
		Headers:        map[string]string{"X-Google": "value"},
	}

	normalized := suite.ingestionService.NormalizeGoogleEmail(tenantID, userID, googleEmail)

	assert.Equal(suite.T(), tenantID, normalized.TenantID)
	assert.Equal(suite.T(), userID, normalized.UserID)
	assert.Equal(suite.T(), emailID.String(), normalized.MessageID)
	assert.Equal(suite.T(), "sender@gmail.com", normalized.From)
	assert.Equal(suite.T(), []string{"recipient@gmail.com"}, normalized.To)
	assert.Equal(suite.T(), "Google Subject", normalized.Subject)
	assert.Equal(suite.T(), "Google Body", normalized.Body)
	assert.Equal(suite.T(), "google", normalized.Provider)
	assert.Equal(suite.T(), receivedAt, normalized.ReceivedAt)
	assert.Equal(suite.T(), map[string]string{"X-Google": "value"}, normalized.Headers)
}

func (suite *IngestionServiceSuite) TestBatchWriter_SmallBatch() {
	ctx := context.Background()

	tenantID := uuid.New()
	userID := uuid.New()

	emails := []domain.Email{
		{
			TenantID:   tenantID,
			UserID:     userID,
			MessageID:  uuid.New().String(),
			From:       "test1@example.com",
			To:         []string{"recipient@example.com"},
			Subject:    "Test 1",
			Body:       "Body 1",
			ReceivedAt: time.Now(),
			Provider:   "microsoft",
		},
		{
			TenantID:   tenantID,
			UserID:     userID,
			MessageID:  uuid.New().String(),
			From:       "test2@example.com",
			To:         []string{"recipient@example.com"},
			Subject:    "Test 2",
			Body:       "Body 2",
			ReceivedAt: time.Now(),
			Provider:   "microsoft",
		},
	}

	// Send emails into a channel
	emailCh := make(chan domain.Email, len(emails))
	for _, email := range emails {
		emailCh <- email
	}
	close(emailCh)

	// Mock the storage to expect a batch
	suite.ingestionStorage.EXPECT().StoreBatch(ctx, mock.MatchedBy(func(batch []domain.Email) bool {
		return len(batch) == 2
	})).Return(nil).Once()

	suite.amqpNotifier.EXPECT().NotifyEmailBatchIngested(ctx, mock.AnythingOfType("*domain.NormalizedEmailsBatchMessage")).Return(nil).Once()

	err := suite.ingestionService.batchWriter(ctx, emailCh, 500)

	assert.NoError(suite.T(), err)
}

func (suite *IngestionServiceSuite) TestBatchWriter_LargeBatch() {
	ctx := context.Background()

	tenantID := uuid.New()
	userID := uuid.New()
	batchSize := 10

	// Create more emails than batch size
	numEmails := 25
	emailCh := make(chan domain.Email, numEmails)
	for i := 0; i < numEmails; i++ {
		emailCh <- domain.Email{
			TenantID:   tenantID,
			UserID:     userID,
			MessageID:  uuid.New().String(),
			From:       "test@example.com",
			To:         []string{"recipient@example.com"},
			Subject:    "Test",
			Body:       "Body",
			ReceivedAt: time.Now(),
			Provider:   "microsoft",
		}
	}
	close(emailCh)

	// Expect 3 batches: 10 + 10 + 5
	suite.ingestionStorage.EXPECT().StoreBatch(ctx, mock.MatchedBy(func(batch []domain.Email) bool {
		return len(batch) == 10
	})).Return(nil).Twice() // Handles the 2 first batches

	suite.ingestionStorage.EXPECT().StoreBatch(ctx, mock.MatchedBy(func(batch []domain.Email) bool {
		return len(batch) == 5
	})).Return(nil).Once()

	suite.amqpNotifier.EXPECT().NotifyEmailBatchIngested(ctx, mock.AnythingOfType("*domain.NormalizedEmailsBatchMessage")).Return(nil).Times(3)

	err := suite.ingestionService.batchWriter(ctx, emailCh, batchSize)

	assert.NoError(suite.T(), err)
}

func (suite *IngestionServiceSuite) TestBatchWriter_StorageError() {

	ctx := context.Background()

	tenantID := uuid.New()
	userID := uuid.New()

	emailCh := make(chan domain.Email, 1)
	emailCh <- domain.Email{
		TenantID:   tenantID,
		UserID:     userID,
		MessageID:  uuid.New().String(),
		From:       "test@example.com",
		To:         []string{"recipient@example.com"},
		Subject:    "Test",
		Body:       "Body",
		ReceivedAt: time.Now(),
		Provider:   "microsoft",
	}
	close(emailCh)

	expectedErr := errors.New("storage error")
	suite.ingestionStorage.EXPECT().StoreBatch(ctx, mock.Anything).Return(expectedErr).Once()

	err := suite.ingestionService.batchWriter(ctx, emailCh, 500)

	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), expectedErr, err)
}

func (suite *IngestionServiceSuite) TestBatchWriter_EmptyChannel() {
	ctx := context.Background()

	emailCh := make(chan domain.Email)
	close(emailCh)

	err := suite.ingestionService.batchWriter(ctx, emailCh, 500)

	assert.NoError(suite.T(), err)
}

func (suite *IngestionServiceSuite) TestExtractEmailIDs() {
	validID1 := uuid.New().String()
	validID2 := uuid.New().String()
	validID3 := uuid.New().String()

	emails := []domain.Email{
		{MessageID: validID1},
		{MessageID: validID2},
		{MessageID: "invalid-uuid"}, // Should be skipped
		{MessageID: validID3},
	}

	ids := extractEmailIDs(emails)

	// Should have 3 valid UUIDs (invalid one is skipped)
	assert.Equal(suite.T(), 3, len(ids))
	assert.Contains(suite.T(), ids, uuid.MustParse(validID1))
	assert.Contains(suite.T(), ids, uuid.MustParse(validID2))
	assert.Contains(suite.T(), ids, uuid.MustParse(validID3))
}

func (suite *IngestionServiceSuite) TestExtractUserIDs() {
	userID1 := uuid.New()
	userID2 := uuid.New()

	emails := []domain.Email{
		{UserID: userID1},
		{UserID: userID2},
		{UserID: userID1}, // Duplicate
		{UserID: userID2}, // Duplicate
		{UserID: userID1}, // Duplicate
	}

	ids := extractUserIDs(emails)

	// Should have only 2 unique user IDs
	assert.Equal(suite.T(), 2, len(ids))
	assert.Contains(suite.T(), ids, userID1)
	assert.Contains(suite.T(), ids, userID2)
}

func (suite *IngestionServiceSuite) TestIngestMicrosoftUser_CursorUpdate_WithoutData() {
	ctx := context.Background()

	tenantID := uuid.New()
	userID := uuid.New()

	baseTime := time.Now().Add(-1 * time.Hour)

	user := domain.MicrosoftUser{
		UserID: userID,
	}

	cursor := &domain.IngestionCursor{
		TenantID:       tenantID,
		Provider:       "microsoft",
		UserID:         userID,
		LastReceivedAt: baseTime,
		UpdatedAt:      baseTime,
	}

	suite.ingestionStorage.EXPECT().LoadCursor(ctx, tenantID, "microsoft", userID).Return(cursor, nil)

	// Empty batch
	suite.ingestionStorage.EXPECT().StoreBatch(ctx, mock.Anything).Return(nil).Maybe()
	suite.amqpNotifier.EXPECT().NotifyEmailBatchIngested(ctx, mock.Anything).Return(nil).Maybe()

	// Verify it's the same cursor (no emails means no update)
	suite.ingestionStorage.EXPECT().UpsertCursor(ctx, mock.MatchedBy(func(c *domain.IngestionCursor) bool {
		return c.TenantID == tenantID && c.UserID == userID && c.LastReceivedAt.Equal(baseTime)
	})).Return(nil)

	err := suite.ingestionService.ingestMicrosoftUser(ctx, tenantID, user)

	assert.NoError(suite.T(), err)
}

func (suite *IngestionServiceSuite) TestIngestMicrosoftUser_LoadCursorError() {

	ctx := context.Background()

	tenantID := uuid.New()
	userID := uuid.New()

	user := domain.MicrosoftUser{
		UserID: userID,
	}

	expectedErr := errors.New("cursor load error")
	suite.ingestionStorage.EXPECT().LoadCursor(ctx, tenantID, "microsoft", userID).Return(nil, expectedErr)

	err := suite.ingestionService.ingestMicrosoftUser(ctx, tenantID, user)

	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), expectedErr, err)
}

func (suite *IngestionServiceSuite) TestIngestGoogleUser_CursorUpdate() {
	ctx := context.Background()

	tenantID := uuid.New()
	userID := uuid.New()

	baseTime := time.Now().Add(-1 * time.Hour)

	user := domain.GoogleUser{
		UserID: userID,
	}

	cursor := &domain.IngestionCursor{
		TenantID:       tenantID,
		Provider:       "google",
		UserID:         userID,
		LastReceivedAt: baseTime,
		UpdatedAt:      baseTime,
	}

	suite.ingestionStorage.EXPECT().LoadCursor(ctx, tenantID, "google", userID).Return(cursor, nil)

	// Empty batch
	suite.ingestionStorage.EXPECT().StoreBatch(ctx, mock.Anything).Return(nil).Maybe()
	suite.amqpNotifier.EXPECT().NotifyEmailBatchIngested(ctx, mock.Anything).Return(nil).Maybe()

	suite.ingestionStorage.EXPECT().UpsertCursor(ctx, mock.MatchedBy(func(c *domain.IngestionCursor) bool {
		return c.TenantID == tenantID && c.UserID == userID
	})).Return(nil)

	err := suite.ingestionService.ingestGoogleUser(ctx, tenantID, user)

	assert.NoError(suite.T(), err)
}

func (suite *IngestionServiceSuite) TestIngestGoogleUser_LoadCursorError() {

	ctx := context.Background()

	tenantID := uuid.New()
	userID := uuid.New()

	user := domain.GoogleUser{
		UserID: userID,
	}

	expectedErr := errors.New("cursor load error")
	suite.ingestionStorage.EXPECT().LoadCursor(ctx, tenantID, "google", userID).Return(nil, expectedErr)

	err := suite.ingestionService.ingestGoogleUser(ctx, tenantID, user)

	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), expectedErr, err)
}
