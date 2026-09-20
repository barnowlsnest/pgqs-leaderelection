package database

import (
	"context"
	"embed"

	"github.com/barnowlsnest/pgqs-harness/v2/db"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

const dirMigrations = "migrations"

// RollOut applies pgqs database migrations.
func RollOut(ctx context.Context, dbURL string) error {
	return db.RollOut(ctx, &embeddedMigrations, dbURL, dirMigrations)
}

// RollDown rolls down pgqs database migrations.
func RollDown(ctx context.Context, dbURL string) error {
	return db.RollDown(ctx, &embeddedMigrations, dbURL, dirMigrations)
}
