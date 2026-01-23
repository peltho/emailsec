package handler

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"

	"stoik.com/emailsec/internal/core/port"
)

type IngestionHTTPHandler struct {
	ingestionService port.IngestionService
	jobQueue         chan IngestTenantRequest
	wg               sync.WaitGroup
	numWorkers       int
}

type IngestTenantRequest struct {
	TenantID uuid.UUID `json:"tenant_id" validate:"required"`
}

type IngestTenantResponse struct {
	Message  string    `json:"message"`
	TenantID uuid.UUID `json:"tenant_id"`
}

func NewIngestionHTTPHandler(
	ingestionService port.IngestionService,
	numWorkers int,
	jobQueueSize int,
) *IngestionHTTPHandler {
	return &IngestionHTTPHandler{
		ingestionService: ingestionService,
		jobQueue:         make(chan IngestTenantRequest, jobQueueSize),
		numWorkers:       numWorkers,
	}
}

func (h *IngestionHTTPHandler) Start(ctx context.Context) {
	for w := range h.numWorkers {
		h.wg.Add(1)
		go func(workerID int) {
			defer h.wg.Done()
			for {
				select {
				case <-ctx.Done():
					log.Warnf("[IngestionWorker %d] Context cancelled, stopping", workerID)
					return
				case req, ok := <-h.jobQueue:
					if !ok {
						return
					}
					newCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
					if err := h.ingestionService.Run(newCtx, req.TenantID); err != nil {
						log.WithError(err).WithField("tenant_id", req.TenantID).Error("Ingestion failed")
					}
					cancel()
				}
			}
		}(w)
	}
}

func (h *IngestionHTTPHandler) Stop(ctx context.Context) {
	close(h.jobQueue)

	workersDone := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
		log.Info("All workers drained in time. Now shutting down")
	case <-ctx.Done():
		log.Info("Drained workers for 5 minutes. Now shutting down.")
	}
}

func (h *IngestionHTTPHandler) Handle() echo.HandlerFunc {
	return func(c echo.Context) error {
		var req IngestTenantRequest

		if err := c.Bind(&req); err != nil {
			log.WithError(err).Error("Failed to bind request")
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Invalid request payload",
			})
		}

		select {
		case h.jobQueue <- req:
			return c.JSON(http.StatusAccepted, IngestTenantResponse{
				Message:  "Ingestion started",
				TenantID: req.TenantID,
			})
		default:
			return c.JSON(http.StatusServiceUnavailable, IngestTenantResponse{
				Message:  "Ingestion paused or unavailable",
				TenantID: req.TenantID,
			})
		}
	}
}
