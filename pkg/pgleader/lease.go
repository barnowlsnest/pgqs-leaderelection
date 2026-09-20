package pgleader

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/barnowlsnest/pgqs-harness/v2/db"
)

// colHashKey is the primary key of the leases table. BaseDAO defaults its
// generic helpers to "id", which this table does not have, so every keyed
// call names the column explicitly.
const colHashKey = "hash_key"

type (
	LeaseRecord struct {
		Holder     string          `json:"holder"      db:"holder"`
		HashKey    int64           `json:"hash_key"    db:"hash_key"`
		Version    int64           `json:"version"     db:"version"`
		AcquiredAt time.Time       `json:"acquired_at" db:"acquired_at"`
		RenewedAt  time.Time       `json:"renewed_at"  db:"renewed_at"`
		ExpiresAt  time.Time       `json:"expires_at"  db:"expires_at"`
		Metadata   json.RawMessage `json:"metadata"    db:"metadata"`
	}

	LeaseRepo struct {
		dao *db.BaseDAO[LeaseRecord]
	}
)

func NewLeaseRepo(dao *db.BaseDAO[LeaseRecord]) (*LeaseRepo, error) {
	if dao == nil {
		return nil, errors.New("dao cannot be nil")
	}
	if err := dao.Validate(); err != nil {
		return nil, err
	}

	return &LeaseRepo{dao: dao}, nil
}

func (r *LeaseRepo) Read(ctx context.Context, key int64) (*LeaseRecord, error) {
	lease, err := r.dao.GetByCol[int64](ctx, colHashKey, key)
	if err != nil {
		return nil, err
	}

	return lease, nil
}

func (r *LeaseRepo) Create(ctx context.Context, lease *LeaseRecord) (*LeaseRecord, error) {
	l, err := r.dao.Create(ctx, lease)
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (r *LeaseRepo) Delete(ctx context.Context, key int64) error {
	return r.dao.DeleteByCol[int64](ctx, colHashKey, key)
}

// Update writes every mutable column of lease back to the row identified by its
// hash key and returns the stored row. hash_key is the identity of the row, so
// the DAO keeps it out of the SET clause.
func (r *LeaseRepo) Update(ctx context.Context, lease *LeaseRecord) (*LeaseRecord, error) {
	if lease == nil {
		return nil, errors.New("lease cannot be nil")
	}

	l, err := r.dao.UpdateWithColName(ctx, colHashKey, lease)
	if err != nil {
		return nil, err
	}

	return l, nil
}
