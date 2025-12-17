package domain

import (
	"time"

	"github.com/google/uuid"
)

var (
	RoutingKeyEmailBatchIngested = "email.batch.ingested"
	RoutingKeyFraudulentDetected = "email.fraud.detected"
)

const (
	EmailExchange           = "email"
	EmailAnalysisQueue      = "email.analysis"
	RoutingKeyEmailIngested = "email.ingested"
)

type NormalizedEmailBatchMessage struct {
	BatchID     uuid.UUID  `json:"batch_id" validate:"required"`
	TenantID    uuid.UUID  `json:"tenant_id" validate:"required"`
	EmailIDList uuid.UUIDs `json:"email_id_list" validate:"required,max=500,dive,required"`
	UserID      uuid.UUID  `json:"user_id" validate:"required"`
	IngestedAt  time.Time  `json:"ingested_at" validate:"required"`
}

type SuspectingFraudulentEmailMessage struct {
	Email      Email
	DetectedAt time.Time
}
