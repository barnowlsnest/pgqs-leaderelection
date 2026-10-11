package database

import (
	"context"
	"embed"
	"errors"

	"github.com/barnowlsnest/pgqs-harness/v2/db"
	"github.com/golang-migrate/migrate/v4"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

const dirMigrations = "migrations"

// RollOut applies pgqs database migrations. A database that is already up to
// date is not an error: RollOut runs on every service start.
func RollOut(ctx context.Context, dbURL string) error {
	return ignoreNoChange(db.RollOut(ctx, &embeddedMigrations, dbURL, dirMigrations))
}

// RollDown rolls down pgqs database migrations. A database with nothing to roll
// back is not an error.
func RollDown(ctx context.Context, dbURL string) error {
	return ignoreNoChange(db.RollDown(ctx, &embeddedMigrations, dbURL, dirMigrations))
}

// ignoreNoChange drops migrate.ErrNoChange, which the harness passes through
// unchanged from golang-migrate.
func ignoreNoChange(err error) error {
	if errors.Is(err, migrate.ErrNoChange) {
		return nil
	}

	return err
}
