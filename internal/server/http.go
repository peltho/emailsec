package server

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	log "github.com/sirupsen/logrus"

	"stoik.com/emailsec/internal/core/domain"
	"stoik.com/emailsec/internal/core/port"
	"stoik.com/emailsec/internal/handler"
)

type HTTPServer struct {
	echo             *echo.Echo
	notifier         port.NotifierClient
	ingestionService port.IngestionService
}

type IngestEmailRequest struct {
	TenantID uuid.UUID    `json:"tenant_id" validate:"required"`
	UserID   uuid.UUID    `json:"user_id" validate:"required"`
	Email    domain.Email `json:"email" validate:"required"`
}

func NewHTTPServer(
	notifier port.NotifierClient,
	ingestionService port.IngestionService,
) *HTTPServer {
	e := echo.New()
	e.HideBanner = true

	// Middleware
	e.Use(middleware.RequestLogger())
	e.Use(middleware.Recover())
	e.Use(middleware.RequestID())

	server := &HTTPServer{
		echo:             e,
		notifier:         notifier,
		ingestionService: ingestionService,
	}

	// Initialize handlers
	ingestionHandler := handler.NewIngestionHTTPHandler(ingestionService)

	// Routes
	e.GET("/health", server.healthCheck)
	e.POST("/api/v1/emails/ingest", ingestionHandler.Handle())

	return server
}

func (s *HTTPServer) healthCheck(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "ingestion",
	})
}

func (s *HTTPServer) Start(address string) error {
	log.Infof("Starting HTTP server on %s", address)
	return s.echo.Start(address)
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	log.Info("Shutting down HTTP server")
	return s.echo.Shutdown(ctx)
}
