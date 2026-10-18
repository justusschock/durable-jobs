# ADR 0009: Supported Go versions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (NFR-JOBS-05), ADR-0010 |

## Context

Recorded retrospectively for the first public release. The tests rely on
`testing/synctest` with its stable API (`synctest.Test`, `synctest.Wait`),
which first shipped in Go 1.25. The code also uses range-over-int and the
`min` builtin. Both modules declare `go 1.25`.

## Decision drivers

- D1 Users on supported Go releases can use the library.
- D2 The test suite must run on the minimum version.
- D3 CI cost stays bounded.

## Considered options

1. **Support only the two upstream-supported Go releases**.
   - Pro: smallest matrix.
   - Con: would raise the floor above 1.25 as soon as 1.27 exists, without a
     technical need.
2. **Go 1.25 as the floor plus the two upstream-supported releases**.
   - Pro: floor fixed by a real requirement (synctest); users on 1.25 keep
     working.
   - Con: one more CI matrix entry while 1.25 is out of upstream support.
3. **A lower floor with build-tagged tests**.
   - Pro: wider reach.
   - Con: virtual-time tests would not run on the floor version.

## Decision

We will declare `go 1.25` in both modules, without a `toolchain` line, and
test on Go 1.25, `oldstable` and `stable` in CI. The floor is raised only by
a new ADR when a required language or library feature demands it.

## Consequences

### Positive
- The floor is explained by a concrete dependency on `testing/synctest`.
- `TestModuleCompatibilityPolicy` guards the `go` directive and the absence
  of `toolchain` and `replace` lines in the core module.

### Negative
- Users on Go 1.24 or older cannot use the library.

### Follow-ups
- Re-evaluate the floor when Go 1.25 is far out of support.

## Links

- [testing/synctest](https://pkg.go.dev/testing/synctest)
