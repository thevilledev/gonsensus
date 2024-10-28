package gonsensus

import (
	"time"
)

type ObserverInfo struct {
	LastHeartbeat time.Time         `json:"lastHeartbeat"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	IsActive      bool              `json:"isActive"`
}
