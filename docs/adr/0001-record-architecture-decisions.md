# ADR 0001: Record architecture decisions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 |

## Context

Recorded retrospectively for the first public release. durable-jobs grew
through many small changes whose rationale lived in commit messages and
private planning notes. A public library needs its requirements and design
decisions in the repository, where contributors and users can read them, and
a process that keeps them current.

## Decision drivers

- D1 Contributors must be able to learn why the code is shaped as it is
  without access to the original author.
- D2 Decisions must not be silently rewritten; the history of a decision is
  itself useful.
- D3 Requirements must be traceable to tests.
- D4 The process must be light enough for a small library.

## Considered options

1. **No written decisions**: rely on code, comments and commit messages.
   - Pro: no overhead.
   - Con: rationale is scattered and lost; alternatives are never recorded.
2. **Design documents in a wiki**.
   - Pro: easy to edit.
   - Con: drifts from the code, is not reviewed with it, and is not versioned
     with releases.
3. **PRDs and MADR-style ADRs in `docs/`, docs-first**.
   - Pro: reviewed in the same pull requests as the code; versioned with it;
     requirement IDs can be cited from tests.
   - Con: some overhead per change.

## Decision

We will record requirements as PRDs in `docs/prd/` and decisions as
MADR-style ADRs in `docs/adr/`, both from the templates in those
directories, and follow a docs-first process: a behaviour change is preceded
by the document change that governs it, in the same pull request.

- New documents start as `Proposed` and become `Accepted` when merged.
- Accepted ADRs are immutable except for their status line. A changed
  decision gets a new ADR with `Supersedes: ADR-NNNN`, and the old ADR's
  status becomes `Superseded by ADR-MMMM`.
- PRDs are kept current; requirement IDs (`FR-JOBS-NN`, `NFR-JOBS-NN`) are
  never reused or renumbered, and tests cite them in comments.
- `docs/README.md` indexes every document with its status.

The initial PRD and ADRs were written retrospectively from the code, tests
and README of v0.7.0 and describe the library as implemented.

## Consequences

### Positive
- Rationale and alternatives are preserved next to the code.
- Requirements map to named tests.

### Negative
- Every behaviour or API change carries documentation work.
- Retrospective ADRs describe evident trade-offs, not a recorded debate.

### Follow-ups
- Keep `docs/README.md` in sync with every added or superseded document.

## Links

- [MADR](https://adr.github.io/madr/)
- [AGENTS.md](../../AGENTS.md)
