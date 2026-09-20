package pgleader

import (
	"context"
	"encoding/json"
	"time"
)

type (
	Lease struct {
		Holder     string          `json:"holder"      db:"holder"`
		HashKey    int64           `json:"hash_key"    db:"hash_key"`
		Version    int64           `json:"version"     db:"version"`
		AcquiredAt time.Time       `json:"acquired_at" db:"acquired_at"`
		RenewedAt  time.Time       `json:"renewed_at"  db:"renewed_at"`
		ExpiresAt  time.Time       `json:"expires_at"  db:"expires_at"`
		Metadata   json.RawMessage `json:"metadata"    db:"metadata"`
	}
	
	DataSource interface {
		WithIDColumn(name string)
		Create(ctx context.Context, entity *Lease) (*Lease, error)
	}
	
	LeaseRepo struct {
		dataSource DataSource
	}
)
