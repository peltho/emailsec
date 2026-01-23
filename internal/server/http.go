package server

import (
	"context"
	"net/http"
	"time"

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
	ingestionHandler *handler.IngestionHTTPHandler
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

	// Initialize handlers
	ingestionHandler := handler.NewIngestionHTTPHandler(ingestionService, 10, 10_000)

	server := &HTTPServer{
		echo:             e,
		notifier:         notifier,
		ingestionService: ingestionService,
		ingestionHandler: ingestionHandler,
	}

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

func (s *HTTPServer) Start(ctx context.Context, address string) error {
	s.ingestionHandler.Start(ctx)
	log.Infof("Starting HTTP server on %s", address)
	return s.echo.Start(address)
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	log.Info("Shutting down HTTP server")
	// First stop accepting new HTTP requests and wait for in-flight requests to complete
	err := s.echo.Shutdown(ctx)
	// Then stop workers and drain the job queue

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Minute)

	s.ingestionHandler.Stop(drainCtx)
	drainCancel()
	return err
}
