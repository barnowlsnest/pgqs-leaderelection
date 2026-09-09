CREATE TABLE IF NOT EXISTS leases (
    id BIGSERIAL PRIMARY KEY,
    resource TEXT NOT NULL,
    holder TEXT NOT NULL,
    fence BIGINT NOT NULL DEFAULT 0,
    acquired_at TIMESTAMPTZ NOT NULL,
    acquired_until TIMESTAMPTZ NOT NULL,
    UNIQUE (resource)
);

CREATE INDEX IF NOT EXISTS idx_leases_acquired_until ON leases (acquired_until);
