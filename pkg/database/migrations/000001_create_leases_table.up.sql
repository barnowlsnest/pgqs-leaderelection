CREATE SCHEMA IF NOT EXISTS pgqs AUTHORIZATION CURRENT_USER;

CREATE TABLE IF NOT EXISTS pgqs.leases (
    hash_key           bigint      PRIMARY KEY,
    holder             text        NOT NULL,
    version            bigint      NOT NULL DEFAULT 1,
    acquired_at        timestamptz NOT NULL DEFAULT now(),
    renewed_at         timestamptz NOT NULL DEFAULT now(),
    expires_at         timestamptz NOT NULL,
    metadata           jsonb
);
