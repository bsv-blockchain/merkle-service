package service

import "context"

// Service defines the daemon lifecycle interface matching Teranode patterns.
type Service interface {
	Init(cfg interface{}) error
	Start(ctx context.Context) error
	Stop() error
	Health() HealthStatus
}

// HealthStatus represents the health of a service component.
type HealthStatus struct {
	Name    string            `json:"name"`
	Status  string            `json:"status"` // "healthy", "degraded", "unhealthy"
	Details map[string]string `json:"details,omitempty"`
}
