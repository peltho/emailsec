package handler

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"

	"stoik.com/emailsec/internal/core/domain"
	"stoik.com/emailsec/internal/core/port"
)

type fraudDetectionJob struct {
	message domain.NormalizedEmailBatchMessage
}

type AMQPConsumer struct {
	fraudDetectionService port.FraudDetectionService
	validate              *validator.Validate
	jobQueue              chan fraudDetectionJob
	wg                    sync.WaitGroup
	numWorkers            int
}

func NewAMQPConsumer(
	fraudDetectionService port.FraudDetectionService,
	validate *validator.Validate,
	numWorkers int,
	queueSize int,
) *AMQPConsumer {
	return &AMQPConsumer{
		fraudDetectionService: fraudDetectionService,
		validate:              validate,
		jobQueue:              make(chan fraudDetectionJob, queueSize),
		numWorkers:            numWorkers,
	}
}

// Start launches the worker pool. Call this before consuming messages.
func (c *AMQPConsumer) Start(ctx context.Context) {
	for i := range c.numWorkers {
		c.wg.Add(1)
		go c.worker(ctx, i)
	}
	log.Infof("Started %d fraud detection workers", c.numWorkers)
}

// Stop gracefully shuts down workers after draining the queue.
func (c *AMQPConsumer) Stop(ctx context.Context) {
	close(c.jobQueue)

	workersDone := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
		log.Info("All fraud detection workers stopped after drained.")
	case <-ctx.Done():
		log.Info("All fraud detection workers stopped after 5 minutes.")
	}
}

func (c *AMQPConsumer) worker(ctx context.Context, workerID int) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Warnf("[FraudWorker %d] Context cancelled, stopping", workerID)
			return
		case job, ok := <-c.jobQueue:
			if !ok {
				log.Infof("[FraudWorker %d] Queue closed, stopping", workerID)
				return
			}
			jobCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			if err := c.fraudDetectionService.Run(jobCtx, job.message); err != nil {
				log.WithError(err).WithField("batchID", job.message.BatchID).Error("Fraud detection failed")
			}
			cancel()
		}
	}
}

func (c *AMQPConsumer) Handle(ctx context.Context, delivery *amqp.Delivery) {
	var err error

	switch delivery.RoutingKey {
	case domain.RoutingKeyEmailBatchIngested:
		err = c.handleEmailBatchIngestedMessage(ctx, delivery)
	default:
		log.Errorf("unsupported routing key %s", delivery.RoutingKey)
	}

	if err != nil {
		delivery.Nack(false, false) // Send to a retry / dead-letter queue instead
		return
	}
	delivery.Ack(false)
}

func (c *AMQPConsumer) handleEmailBatchIngestedMessage(_ context.Context, delivery *amqp.Delivery) error {
	var batchMessage domain.NormalizedEmailBatchMessage

	if err := json.Unmarshal(delivery.Body, &batchMessage); err != nil {
		log.Errorf("failed to unmarshal email batch message: %v", err)
		return err
	}

	// Validate the message
	if err := c.validate.Struct(batchMessage); err != nil {
		log.Errorf("email batch message validation failed: %v", err)
		return err
	}

	log.WithFields(log.Fields{
		"tenantID":   batchMessage.TenantID,
		"batchID":    batchMessage.BatchID,
		"userID":     batchMessage.UserID,
		"emailCount": len(batchMessage.EmailIDList),
		"ingestedAt": batchMessage.IngestedAt,
	}).Info("Received email batch for fraud detection")

	// Submit to worker pool (blocks if queue is full, providing backpressure)
	c.jobQueue <- fraudDetectionJob{message: batchMessage}

	return nil
}
