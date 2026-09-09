package pgleader

import (
	"time"
)

type Lease struct {
	ID            uint64    `db:"id"`
	Fence         uint64    `db:"fence"`
	Resource      string    `db:"resource"`
	Holder        string    `db:"holder"`
	AcquiredAt    time.Time `db:"acquired_at"`
	AcquiredUntil time.Time `db:"acquired_until"`
}
