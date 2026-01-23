package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"stoik.com/emailsec/internal/client"
	"stoik.com/emailsec/internal/core/service"
	"stoik.com/emailsec/internal/infrastructure/amqp"
	"stoik.com/emailsec/internal/server"
	"stoik.com/emailsec/internal/storage"
)

func main() {
	// Initialize logger
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.InfoLevel)

	// Get configuration from environment
	amqpURL := os.Getenv("AMQP_URL")
	httpAddr := os.Getenv("HTTP_ADDR")
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
	notifier := client.NewAMQPNotifier(*publisher)

	ctx := context.Background()
	db, err := storage.NewPostgresDB(ctx, dbHost, dbPort, dbUser, dbPassword, dbName)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()
	storage := storage.NewEmailsStorage(db)

	// Set up topology (exchanges, queues, bindings)
	topologyManager := amqp.NewTopologyManager(amqpClient)
	if err := topologyManager.Setup(); err != nil {
		log.Fatalf("Failed to setup AMQP topology: %v", err)
	}

	ingestionService := service.NewIngestionService(storage, notifier)

	// Create HTTP server
	httpServer := server.NewHTTPServer(notifier, ingestionService)

	// Create cancellable context for worker lifecycle
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	// Start HTTP server in a goroutine
	go func() {
		if err := httpServer.Start(workerCtx, httpAddr); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	log.Info("Ingestion service started successfully")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("Shutting down ingestion service...")

	// Graceful shutdown with timeout
	// Workers will drain the queue before stopping
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Errorf("Error shutting down HTTP server: %v", err)
	}
}
