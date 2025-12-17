package domain

import (
	"time"

	"github.com/google/uuid"
)

var (
	RoutingKeyEmailIngested      = "email.ingested"
	RoutingKeyFraudulentDetected = "email.fraud.detected"
)

type NormalizedEmailsBatchMessage struct {
	BatchID     uuid.UUID
	TenantID    uuid.UUID
	EmailIDList uuid.UUIDs
	UserIDList  uuid.UUIDs
	IngestedAt  time.Time
}

type SuspectingFraudulentEmailMessage struct {
	Email      Email
	DetectedAt time.Time
}
