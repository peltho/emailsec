package service

import (
	"context"

	"stoik.com/emailsec/internal/core/domain"
)

type FraudDetectionService struct {
}

func NewFraudDetectionService() *FraudDetectionService {
	return &FraudDetectionService{}
}

func (f *FraudDetectionService) AnalyzeEmail(ctx context.Context, email domain.Email) (bool, error) {
	return false, nil
}
