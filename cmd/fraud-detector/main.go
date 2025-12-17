package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"

	"stoik.com/emailsec/internal/client"
	"stoik.com/emailsec/internal/handler"
	"stoik.com/emailsec/internal/infrastructure/amqp"
)

func main() {
	// Initialize logger
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.InfoLevel)

	// Get RabbitMQ URL from environment
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}

	// Create AMQP client
	amqpClient, err := amqp.NewClient(amqpURL)
	if err != nil {
		log.Fatalf("Failed to create AMQP client: %v", err)
	}
	defer amqpClient.Close()

	// Set up topology (exchanges, queues, bindings)
	topologyManager := amqp.NewTopologyManager(amqpClient)
	if err := topologyManager.Setup(); err != nil {
		log.Fatalf("Failed to setup AMQP topology: %v", err)
	}

	// Create publisher
	publisher := amqp.NewPublisher(amqpClient)

	// Create notifier
	notifier := client.NewAMQPNotifier(publisher)

	// TODO: Initialize your detection service and email storage
	// For now, use nil interfaces - you'll need to implement these with real services
	// Example:
	// detectionService := service.NewFraudDetectionService()
	// emailStorage := storage.NewPostgresEmailStorage(db)

	// Create validator
	validate := validator.New()

	// Create message handler (consumer handler)
	// Note: passing nil for services - replace with actual implementations
	messageHandler := handler.NewAMQPConsumer(
		nil, // detectionService - implement port.DetectionService
		nil, // emailStorage - implement port.EmailStorage
		validate,
	)

	// Create consumer
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
