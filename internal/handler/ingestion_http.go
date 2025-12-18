package handler

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"

	"stoik.com/emailsec/internal/core/port"
)

type IngestionHTTPHandler struct {
	ingestionService port.IngestionService
}

type IngestTenantRequest struct {
	TenantID uuid.UUID `json:"tenant_id" validate:"required"`
}

type IngestTenantResponse struct {
	Message  string    `json:"message"`
	TenantID uuid.UUID `json:"tenant_id"`
}

func NewIngestionHTTPHandler(ingestionService port.IngestionService) *IngestionHTTPHandler {
	return &IngestionHTTPHandler{
		ingestionService: ingestionService,
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

		// Run ingestion asynchronously since it can take time
		go func() {
			newCtx := context.Background()
			if err := h.ingestionService.Run(newCtx, req.TenantID); err != nil {
				log.WithError(err).WithField("tenant_id", req.TenantID).Error("Ingestion failed")
			}
		}()

		return c.JSON(http.StatusAccepted, IngestTenantResponse{
			Message:  "Ingestion started",
			TenantID: req.TenantID,
		})
	}
}
