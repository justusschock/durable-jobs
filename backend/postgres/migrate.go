package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// migration is one embedded, versioned schema change.
type migration struct {
	version int
	name    string
	sql     string
}

const createMigrationsTableSQL = `CREATE TABLE IF NOT EXISTS {{schema}}.schema_migrations (
    version integer PRIMARY KEY,
    name text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)`

// Migrate idempotently applies every embedded schema migration that the
// configured schema has not recorded yet. It creates the schema when it does
// not exist. Concurrent callers, including other processes, serialize on a
// transaction-scoped advisory lock, and all pending migrations commit
// atomically in one transaction. Migrations recorded by a newer release are
// left untouched, so older replicas can keep running during a rolling upgrade.
func (store *Store) Migrate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, store.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "go-durable-jobs/postgres:"+store.schemaName); err != nil {
			return err
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1)`, store.schemaName).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			if _, err := tx.Exec(ctx, "CREATE SCHEMA "+store.schema); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, store.sql(createMigrationsTableSQL)); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, store.sql(`SELECT version FROM {{schema}}.schema_migrations`))
		if err != nil {
			return err
		}
		applied, err := pgx.CollectRows(rows, pgx.RowTo[int32])
		if err != nil {
			return err
		}
		done := make(map[int]bool, len(applied))
		for _, version := range applied {
			done[int(version)] = true
		}
		for _, migration := range migrations {
			if done[migration.version] {
				continue
			}
			if _, err := tx.Exec(ctx, store.sql(migration.sql)); err != nil {
				return fmt.Errorf("durable jobs postgres: migration %d (%s): %w", migration.version, migration.name, err)
			}
			if _, err := tx.Exec(ctx, store.sql(`INSERT INTO {{schema}}.schema_migrations (version, name) VALUES ($1, $2)`), migration.version, migration.name); err != nil {
				return err
			}
		}
		return nil
	})
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, err
	}
	migrations := make([]migration, 0, len(entries))
	seen := make(map[int]string, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		prefix, rest, found := strings.Cut(strings.TrimSuffix(name, ".sql"), "_")
		version, err := strconv.Atoi(prefix)
		if !found || err != nil || version <= 0 || rest == "" {
			return nil, fmt.Errorf("durable jobs postgres: invalid migration file name %q", name)
		}
		if previous, duplicate := seen[version]; duplicate {
			return nil, fmt.Errorf("durable jobs postgres: migrations %q and %q share version %d", previous, name, version)
		}
		seen[version] = name
		contents, err := migrationFiles.ReadFile(path.Join("migrations", name))
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, migration{version: version, name: rest, sql: string(contents)})
	}
	sort.Slice(migrations, func(left, right int) bool {
		return migrations[left].version < migrations[right].version
	})
	return migrations, nil
}
