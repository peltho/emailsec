package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"

	"stoik.com/emailsec/internal/client"
	"stoik.com/emailsec/internal/core/service"
	"stoik.com/emailsec/internal/handler"
	"stoik.com/emailsec/internal/infrastructure/amqp"
	"stoik.com/emailsec/internal/storage"
)

func main() {
	// Initialize logger
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.InfoLevel)

	// Get configuration from environment
	amqpURL := os.Getenv("AMQP_URL")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")

	// Create AMQP client
	amqpClient, err := amqp.NewClient(amqpURL)
	if err != nil {
		log.Fatalf("Failed to create AMQP client: %v", err)
	}
	defer amqpClient.Close()
	publisher := amqp.NewPublisher(amqpClient)
	notifier := client.NewAMQPNotifier(publisher)

	ctx := context.Background()
	db, err := storage.NewPostgresDB(ctx, dbHost, dbPort, dbUser, dbPassword, dbName)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()
	emailsStorage := storage.NewEmailsStorage(db)

	// Set up topology (exchanges, queues, bindings)
	topologyManager := amqp.NewTopologyManager(amqpClient)
	if err := topologyManager.Setup(); err != nil {
		log.Fatalf("Failed to setup AMQP topology: %v", err)
	}
	validate := validator.New()
	fraudDetectionService := service.NewFraudDetectionService(emailsStorage)
	messageHandler := handler.NewAMQPConsumer(
		fraudDetectionService,
		validate,
	)

	consumer := amqp.NewConsumer(amqpClient, messageHandler)

	// Start consuming messages
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := consumer.Consume(ctx, amqp.EmailAnalysisQueue); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	log.Info("Fraud detection service started successfully")
	log.Infof("Consuming messages from queue: %s", amqp.EmailAnalysisQueue)

	// You can use the notifier to send messages when fraud is detected
	_ = notifier

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("Shutting down fraud detection service...")
	cancel()
}
