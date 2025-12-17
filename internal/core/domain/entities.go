package domain

import (
	"time"

	"github.com/google/uuid"
)

type Tenant struct {
	TenantID uuid.UUID
	Provider string // microsoft or google
	Name     string // Voyage Privé, api.video, etc.
}

type MicrosoftUser struct {
	UserID uuid.UUID
}

type GoogleUser struct {
	UserID uuid.UUID
}

type MicrosoftEmail struct {
	EmailID        uuid.UUID
	ReceivedAt     time.Time
	HasAttachments bool
	From           string
	To             []string
	Subject        string
	Body           string
	Headers        map[string]string
}

type GoogleEmail struct {
	EmailID        uuid.UUID
	ReceivedAt     time.Time
	HasAttachments bool
	From           string
	To             []string
	Subject        string
	Body           string
	Headers        map[string]string
}

type Email struct {
	TenantID   uuid.UUID
	UserID     uuid.UUID
	MessageID  string
	From       string
	To         []string
	Subject    string
	Body       string
	Headers    map[string]string
	ReceivedAt time.Time
	Provider   string
}

type IngestionCursor struct {
	TenantID       uuid.UUID
	Provider       string
	UserID         uuid.UUID
	LastReceivedAt time.Time
	UpdatedAt      time.Time
}
