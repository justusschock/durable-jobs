module github.com/justusschock/go-durable-jobs/backend/postgres

go 1.25.0

require (
	github.com/jackc/pgx/v5 v5.11.0
	github.com/justusschock/go-distributed-coordination v0.2.0
	github.com/justusschock/go-durable-jobs v0.7.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/justusschock/go-concurrency v0.2.0 // indirect
	github.com/justusschock/go-messaging v0.2.0 // indirect
	github.com/justusschock/go-reconciliation v0.2.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/justusschock/go-durable-jobs => ../..
