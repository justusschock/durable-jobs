# go-durable-jobs documentation

Requirements (PRDs) state *what* the library does; architecture decision
records (ADRs) state *how and why*. The process is described in
[ADR-0001](adr/0001-record-architecture-decisions.md) and
[AGENTS.md](../AGENTS.md). New documents start from the templates:
[PRD template](prd/TEMPLATE.md), [ADR template](adr/TEMPLATE.md).

## Product requirements

| PRD | Title | Status | Implementation |
|---|---|---|---|
| [PRD-0001](prd/0001-go-durable-jobs.md) | go-durable-jobs | Accepted | Complete |

## Architecture decisions

| ADR | Title | Status |
|---|---|---|
| [ADR-0001](adr/0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [ADR-0002](adr/0002-package-and-api-shape.md) | Package and API shape | Accepted |
| [ADR-0003](adr/0003-dependency-policy.md) | Dependency policy | Accepted |
| [ADR-0004](adr/0004-error-semantics.md) | Error semantics | Accepted |
| [ADR-0005](adr/0005-fenced-attempts-concurrency-and-cancellation.md) | Fenced attempts, concurrency and cancellation | Accepted |
| [ADR-0006](adr/0006-pluggable-stores-and-contract-suites.md) | Pluggable stores, shared state machine and contract suites | Accepted |
| [ADR-0007](adr/0007-persistence-formats-and-nested-postgres-module.md) | Persistence formats and the nested PostgreSQL module | Accepted |
| [ADR-0008](adr/0008-public-api-boundary.md) | Public API boundary | Accepted |
| [ADR-0009](adr/0009-supported-go-versions.md) | Supported Go versions | Accepted |
| [ADR-0010](adr/0010-testing-strategy.md) | Testing strategy | Accepted |
| [ADR-0011](adr/0011-recovery-and-schedules-as-idempotent-reconciliation.md) | Recovery and schedules as idempotent, fenced reconciliation | Accepted |
