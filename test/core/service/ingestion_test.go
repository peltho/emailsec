package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest/v3"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"stoik.com/emailsec/internal/client"
	"stoik.com/emailsec/internal/core/domain"
	"stoik.com/emailsec/internal/core/service"
	infraamqp "stoik.com/emailsec/internal/infrastructure/amqp"
	"stoik.com/emailsec/internal/storage"
	"stoik.com/emailsec/mocks"
	"stoik.com/emailsec/test"
)

func TestIngestion(t *testing.T) {
	suite.Run(t, new(IngestionSuite))
}

type IngestionSuite struct {
	suite.Suite
	dockerPool       *dockertest.Pool
	postgresResource *dockertest.Resource
	rmqResource      *dockertest.Resource
	postgresDB       *sql.DB
	ingestionService *service.IngestionService
	storage          *storage.EmailsStorage
	rmqConnection    *amqp.Connection
}

func (suite *IngestionSuite) SetupSuite() {
	pool, err := dockertest.NewPool("")
	if err != nil {
		suite.T().Fatalf("Could not connect to docker: %s", err)
	}
	suite.dockerPool = pool

	db, port, pgResource := test.SetupPostgresDB(suite.T(), pool)
	suite.postgresDB = db
	suite.postgresResource = pgResource

	conn, rmqResource := test.SetupRabbitMQ(suite.T(), pool)
	suite.rmqConnection = conn
	suite.rmqResource = rmqResource

	if !suite.T().Failed() {
		ctx := context.Background()
		postgresDB, err := storage.NewPostgresDB(ctx, test.PostgresHost, port, test.PostgresUser, test.PostgresPassword, test.PostgresDB)
		if err != nil {
			suite.T().Fatalf("Failed to connect to database: %v", err)
		}

		suite.storage = storage.NewEmailsStorage(postgresDB)

		// Create mock notifier that accepts any calls
		mockNotifier := mocks.NewNotifierClient(suite.T())
		mockNotifier.On("NotifyEmailBatchIngested", context.Background(), mock.Anything).Return(nil).Maybe()

		suite.ingestionService = service.NewIngestionService(suite.storage, mockNotifier)
	}
}

func (suite *IngestionSuite) SetupTest() {
	test.ExecFile(suite.T(), suite.postgresDB, "../../sql/create_tables.sql")
	test.ExecFile(suite.T(), suite.postgresDB, "../../sql/fixtures.sql")

	if suite.T().Failed() {
		suite.T().FailNow()
	}
}

func (suite *IngestionSuite) TearDownSuite() {
	if suite.postgresDB != nil {
		_ = suite.postgresDB.Close()
	}
	if suite.rmqConnection != nil {
		_ = suite.rmqConnection.Close()
	}
	if suite.dockerPool != nil {
		if suite.postgresResource != nil {
			_ = suite.dockerPool.Purge(suite.postgresResource)
		}
		if suite.rmqResource != nil {
			_ = suite.dockerPool.Purge(suite.rmqResource)
		}
	}
}

func (suite *IngestionSuite) TestRun_Microsoft_NoEmails() {
	ctx := context.Background()

	tenantID, _ := uuid.Parse("d290f1ee-6c54-4b01-90e6-d701748f0851")

	// This test verifies that running ingestion with no emails works correctly
	// The GetMicrosoftEmails stub returns nil, so no emails should be ingested
	err := suite.ingestionService.Run(ctx, tenantID)
	suite.NoError(err)

	// Verify no emails were stored
	var count int
	err = suite.postgresDB.QueryRow("SELECT COUNT(*) FROM emails WHERE tenant_id = $1", tenantID).Scan(&count)
	suite.NoError(err)
	suite.Equal(0, count, "Expected no emails to be stored when GetMicrosoftEmails returns empty")
}

func (suite *IngestionSuite) TestRun_Google_NoEmails() {
	ctx := context.Background()

	tenantID, _ := uuid.Parse("c4b1d2f3-3c4b-4f5a-8e9d-0a1b2c3d4e5f")

	// This test verifies that running ingestion with Google tenant works
	err := suite.ingestionService.Run(ctx, tenantID)
	suite.NoError(err)

	// Verify no emails were stored
	var count int
	err = suite.postgresDB.QueryRow("SELECT COUNT(*) FROM emails WHERE tenant_id = $1", tenantID).Scan(&count)
	suite.NoError(err)
	suite.Equal(0, count, "Expected no emails to be stored when GetGoogleEmails returns empty")
}

func (suite *IngestionSuite) TestRun_InvalidTenant() {
	ctx := context.Background()

	invalidTenantID := uuid.New()

	// This should fail because the tenant doesn't exist in fixtures
	err := suite.ingestionService.Run(ctx, invalidTenantID)
	suite.Error(err, "Expected error when tenant doesn't exist")
}

func (suite *IngestionSuite) TestCursorManagement() {
	ctx := context.Background()

	tenantID, _ := uuid.Parse("d290f1ee-6c54-4b01-90e6-d701748f0851")
	userID := uuid.New()

	// Insert initial cursor
	initialTime := time.Now().UTC().Add(-1 * time.Hour)
	_, err := suite.postgresDB.Exec(`
		INSERT INTO ingestion_cursors (tenant_id, provider, user_id, last_received_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
	`, tenantID, "microsoft", userID, initialTime, initialTime)
	suite.NoError(err)

	// Load cursor through storage
	cursor, err := suite.storage.LoadCursor(ctx, tenantID, "microsoft", userID)
	suite.NoError(err)
	suite.NotNil(cursor)
	suite.Equal(tenantID, cursor.TenantID)
	suite.Equal("microsoft", cursor.Provider)
	suite.Equal(userID, cursor.UserID)
	suite.WithinDuration(initialTime, cursor.LastReceivedAt.UTC(), time.Second)

	// Update cursor
	newTime := time.Now().UTC()
	cursor.LastReceivedAt = newTime
	cursor.UpdatedAt = newTime
	err = suite.storage.UpsertCursor(ctx, cursor)
	suite.NoError(err)

	// Verify cursor was updated
	var lastReceivedAt time.Time
	err = suite.postgresDB.QueryRow(`
		SELECT last_received_at FROM ingestion_cursors
		WHERE tenant_id = $1 AND provider = $2 AND user_id = $3
	`, tenantID, "microsoft", userID).Scan(&lastReceivedAt)
	suite.NoError(err)
	suite.WithinDuration(newTime, lastReceivedAt.UTC(), time.Second)
}

func (suite *IngestionSuite) TestBatchWriter_NotifiesOnBatch() {
	ctx := context.Background()

	tenantID, _ := uuid.Parse("d290f1ee-6c54-4b01-90e6-d701748f0851")
	userID := uuid.New()

	// Create mock notifier that tracks calls
	mockNotifier := mocks.NewNotifierClient(suite.T())
	mockNotifier.On("NotifyEmailBatchIngested", mock.Anything, mock.MatchedBy(func(msg *domain.NormalizedEmailBatchMessage) bool {
		return len(msg.EmailIDList) == 10 && msg.TenantID == tenantID && msg.UserID == userID
	})).Return(nil)

	// Create a custom ingestion service with the mock notifier
	testService := service.NewIngestionService(suite.storage, mockNotifier)

	// Create a channel to send test emails
	emailCh := make(chan domain.Email, 10)

	// Start the batch writer in a goroutine as the "real" one would do
	errCh := make(chan error, 1)
	go func() {
		errCh <- testService.BatchWriter(ctx, tenantID, userID, emailCh, 500)
	}()

	// Send 10 test emails through the channel
	baseTime := time.Now().UTC()
	for i := range 10 {
		email := domain.Email{
			TenantID:   tenantID,
			UserID:     userID,
			MessageID:  uuid.New().String(),
			From:       "sender@example.com",
			To:         []string{"recipient@example.com"},
			Subject:    "Test Email",
			Body:       "Test body",
			Headers:    map[string]string{},
			ReceivedAt: baseTime.Add(time.Duration(i) * time.Minute),
			Provider:   "microsoft",
		}
		emailCh <- email
	}
	close(emailCh)

	// Wait for batch writer to complete
	err := <-errCh
	suite.NoError(err)

	// Verify emails were stored
	var count int
	err = suite.postgresDB.QueryRow("SELECT COUNT(*) FROM emails WHERE tenant_id = $1 AND user_id = $2", tenantID, userID).Scan(&count)
	suite.NoError(err)
	suite.Equal(10, count, "Expected 10 emails to be stored")

	// Verify the notifier was called by the actual BatchWriter method
	mockNotifier.AssertCalled(suite.T(), "NotifyEmailBatchIngested", mock.Anything, mock.Anything)

	// Verify it was called exactly once
	mockNotifier.AssertNumberOfCalls(suite.T(), "NotifyEmailBatchIngested", 1)
}

func (suite *IngestionSuite) TestBatchWriter_NotifiesOnBatch_RabbitMQ() {
	ctx := context.Background()

	tenantID, _ := uuid.Parse("d290f1ee-6c54-4b01-90e6-d701748f0851")
	userID := uuid.New()

	ch, err := suite.rmqConnection.Channel()
	suite.NoError(err)

	// Setup topology manually: declare exchange
	err = ch.ExchangeDeclare(
		domain.EmailExchange,
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	suite.NoError(err)

	// Declare queue
	queue, err := ch.QueueDeclare(
		domain.EmailAnalysisQueue, // queue name
		true,                      // durable
		false,                     // delete when unused
		false,                     // exclusive
		false,                     // no-wait
		nil,                       // arguments
	)
	suite.NoError(err)

	// Bind queue to exchange
	err = ch.QueueBind(
		domain.EmailAnalysisQueue,
		domain.RoutingKeyEmailBatchIngested,
		domain.EmailExchange,
		false, // no-wait
		nil,   // arguments
	)
	suite.NoError(err)

	// Purge the queue to ensure clean state (should already be empty)
	_, err = ch.QueuePurge(queue.Name, false)
	suite.NoError(err)

	// Create AMQP client wrapper using the existing connection
	rmqPort := suite.rmqResource.GetPort("5672/tcp")
	rmqURL := test.RabbitMQURL(rmqPort)
	amqpClient, err := infraamqp.NewClient(rmqURL)
	suite.NoError(err)
	defer amqpClient.Close()

	// Create real publisher and notifier
	publisher := infraamqp.NewPublisher(amqpClient)
	notifier := client.NewAMQPNotifier(*publisher)

	// Create ingestion service with real notifier
	testService := service.NewIngestionService(suite.storage, notifier)

	// Create a channel to send test emails
	emailCh := make(chan domain.Email, 10)

	// Start the batch writer in a goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- testService.BatchWriter(ctx, tenantID, userID, emailCh, 500)
	}()

	// Send 10 test emails through the channel
	baseTime := time.Now().UTC()
	for i := range 10 {
		email := domain.Email{
			TenantID:   tenantID,
			UserID:     userID,
			MessageID:  uuid.New().String(),
			From:       "sender@example.com",
			To:         []string{"recipient@example.com"},
			Subject:    "Test Email",
			Body:       "Test body",
			Headers:    map[string]string{},
			ReceivedAt: baseTime.Add(time.Duration(i) * time.Minute),
			Provider:   "microsoft",
		}
		emailCh <- email
	}
	close(emailCh)

	// Wait for batch writer to complete
	err = <-errCh
	suite.NoError(err)

	// Verify emails were stored
	var count int
	err = suite.postgresDB.QueryRow("SELECT COUNT(*) FROM emails WHERE tenant_id = $1 AND user_id = $2", tenantID, userID).Scan(&count)
	suite.NoError(err)
	suite.Equal(10, count, "Expected 10 emails to be stored")

	// Wait a bit for message to be published
	time.Sleep(100 * time.Millisecond)

	// Consume and verify the message content
	msgs, err := ch.Consume(
		domain.EmailAnalysisQueue, // queue
		"",                        // consumer
		true,                      // auto-ack
		false,                     // exclusive
		false,                     // no-local
		false,                     // no-wait
		nil,                       // args
	)
	suite.NoError(err)

	// Wait for the message with timeout
	select {
	case msg := <-msgs:
		suite.NotNil(msg.Body, "Expected message body")

		// Decode and verify the message structure
		var batchMsg domain.NormalizedEmailBatchMessage
		err = json.Unmarshal(msg.Body, &batchMsg)
		suite.NoError(err, "Failed to unmarshal message body")

		suite.Equal(tenantID, batchMsg.TenantID, "TenantID mismatch")
		suite.Equal(userID, batchMsg.UserID, "UserID mismatch")
		suite.Equal(10, len(batchMsg.EmailIDList), "Expected 10 emails in batch")
	case <-time.After(2 * time.Second):
		suite.Fail("Timeout waiting for message from RabbitMQ")
	}
}

func (suite *IngestionSuite) TestEmailStorage() {
	ctx := context.Background()

	tenantID, _ := uuid.Parse("d290f1ee-6c54-4b01-90e6-d701748f0851")
	userID := uuid.New()

	emails := []domain.Email{
		{
			TenantID:   tenantID,
			UserID:     userID,
			MessageID:  uuid.New().String(),
			From:       "sender1@example.com",
			To:         []string{"recipient1@example.com", "recipient2@example.com"},
			Subject:    "Test Email 1",
			Body:       "This is a test email body",
			Headers:    map[string]string{"X-Custom-Header": "value1"},
			ReceivedAt: time.Now(),
			Provider:   "microsoft",
		},
		{
			TenantID:   tenantID,
			UserID:     userID,
			MessageID:  uuid.New().String(),
			From:       "sender2@example.com",
			To:         []string{"recipient3@example.com"},
			Subject:    "Test Email 2",
			Body:       "Another test email",
			Headers:    map[string]string{"X-Another-Header": "value2"},
			ReceivedAt: time.Now().Add(1 * time.Minute),
			Provider:   "microsoft",
		},
	}

	// Store batch
	err := suite.storage.StoreBatch(ctx, emails)
	suite.NoError(err)

	// Verify emails were stored
	var count int
	err = suite.postgresDB.QueryRow(
		"SELECT COUNT(*) FROM emails WHERE tenant_id = $1 AND user_id = $2",
		tenantID, userID,
	).Scan(&count)
	suite.NoError(err)
	suite.Equal(2, count, "Expected 2 emails to be stored")

	// Verify email details
	var subject string
	var fromAddress string
	err = suite.postgresDB.QueryRow(
		"SELECT subject, from_address FROM emails WHERE message_id = $1",
		emails[0].MessageID,
	).Scan(&subject, &fromAddress)
	suite.NoError(err)
	suite.Equal("Test Email 1", subject)
	suite.Equal("sender1@example.com", fromAddress)
}
