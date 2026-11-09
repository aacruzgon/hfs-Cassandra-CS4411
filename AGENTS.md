# AGENTS.md

This file provides guidance to Codex when working with code in this repository.

## Architecture Overview

### Workspace Structure

The project is a Rust workspace with 22 crates (21 default-members; `pysof` excluded from the default build). Counted by hand from `crates/*/Cargo.toml` and the root `Cargo.toml`'s `[workspace] default-members`, cross-checked with `cargo metadata --no-deps --offline` — no build required. This table had drifted from the workspace (missing `helios-fhir-validator`, `helios-observability`, `helios-ui`, `helios-ui-chrome`, `helios-hts-ui`, and the `validator-cli` binary); prefer deriving the count over trusting this prose, see `.hfs-monitor/project.md`.

| Crate | Description |
|-------|-------------|
| **`helios-fhir`** | Core FHIR data models (auto-generated). Supports R4, R4B, R5, R6 via feature flags. |
| **`helios-fhir-gen`** | Code generator - produces Rust structs from FHIR JSON schemas. R6 specs auto-downloaded. |
| **`helios-fhir-macro`** | Procedural macros for FHIR functionality. |
| **`helios-fhirpath`** | FHIRPath expression language - parser (chumsky), evaluator, CLI tool, and HTTP server. |
| **`helios-fhirpath-support`** | Shared support utilities for FHIRPath. |
| **`helios-fhir-validator`** | FHIR resource validation - FHIR Schema based structural/profile engine, SD->schema converter, embedded core packs (R4-R6), deferred FHIRPath-constraint and terminology-binding effects. Configured via `HFS_VALIDATION_*`. |
| **`helios-serde`** | JSON and XML serialization for FHIR resources (`xml` feature flag). |
| **`helios-serde-support`** | Shared serde helpers. |
| **`helios-rest`** | FHIR RESTful API layer (Axum) - handlers, middleware, extractors, multi-tenancy routing. |
| **`helios-persistence`** | Polyglot persistence - backends (SQLite, PostgreSQL, Elasticsearch, MongoDB), composite storage, search registry, tenant isolation. |
| **`helios-hfs`** | Main FHIR server binary. Combines `helios-rest` with storage backends. |
| **`helios-sof`** | SQL-on-FHIR implementation - ViewDefinition processing, CLI and HTTP server. |
| **`helios-hts`** | FHIR Terminology Server (HTS) - CodeSystem/ValueSet/ConceptMap operations and terminology import (SNOMED, LOINC, RxNorm, ICD-10-CM). Provides the `hts` binary. |
| **`helios-auth`** | Authentication & authorization - SMART-on-FHIR / OAuth2 JWT bearer validation, JWKS, scopes. Configured via `HFS_AUTH_*`. |
| **`helios-audit`** | Audit logging - FHIR AuditEvent with IHE BALP profiles; pluggable sinks (database, file, CloudWatch, S3). Configured via `HFS_AUDIT_*`. |
| **`helios-subscriptions`** | FHIR topic-based Subscriptions engine - rest-hook, websocket, email, and messaging channels. Configured via `HFS_SUBSCRIPTION(S)_*`. |
| **`helios-cds-hooks`** | CDS Hooks protocol types and async service trait (HL7 CDS Hooks v3.0.0-ballot). Standalone library. |
| **`helios-observability`** | Shared observability wiring (uptime, Prometheus `/metrics`, OTLP traces) for Helios servers. |
| **`helios-ui`** (`crates/ui`) | Optional server-rendered HTMX web UI for HFS - Askama templates, vendored/pinned htmx, vanilla JS assets, no SPA framework and no runtime CDN. |
| **`helios-ui-chrome`** (`crates/ui-chrome`) | Shared Askama chrome partials for the HFS and HTS web UIs - the topbar account menu and Capability Statement projection, so `helios-ui` and `helios-hts-ui` link the same markup instead of each keeping a copy. |
| **`helios-hts-ui`** (`crates/hts-ui`) | Optional server-rendered HTMX administrative UI for the Helios Terminology Server (HTS), built on `helios-ui-chrome`. |
| **`pysof`** | Python bindings (PyO3/maturin) for SQL-on-FHIR. Excluded from default workspace build. |

### Binaries

| Binary | Crate | Description |
|--------|-------|-------------|
| `hfs` | helios-hfs | FHIR server |
| `fhirpath-cli` | helios-fhirpath | FHIRPath expression evaluator CLI |
| `fhirpath-server` | helios-fhirpath | FHIRPath HTTP evaluation server |
| `sof-cli` | helios-sof | SQL-on-FHIR CLI tool |
| `sof-server` | helios-sof | SQL-on-FHIR HTTP server |
| `config-advisor` | helios-persistence | Storage configuration advisor |
| `hts` | helios-hts | FHIR Terminology Server (HTS) |
| `validator-cli` | helios-fhir-validator | FHIR resource validator CLI (`cli` feature) |

### Key Design Patterns

#### Version-Agnostic Abstraction
The codebase uses enum wrappers and traits to handle multiple FHIR versions:

```rust
// Example from sof crate
pub enum SofViewDefinition {
    R4(fhir::r4::ViewDefinition),
    R4B(fhir::r4b::ViewDefinition),
    R5(fhir::r5::ViewDefinition),
    R6(fhir::r6::ViewDefinition),
}
```

#### Trait-Based Processing
Core functionality is defined through traits, allowing version-independent logic:
- `ViewDefinitionTrait`, `BundleTrait`, `ResourceTrait` (SOF)
- `ResourceStorage`, `VersionedStorage`, `SearchProvider`, `Transaction` (persistence)

#### Persistence Trait Hierarchy
Storage backends implement a progressive trait hierarchy:
```
ResourceStorage -> VersionedStorage -> InstanceHistoryProvider -> TypeHistoryProvider -> SystemHistoryProvider
ResourceStorage -> SearchProvider -> MultiTypeSearchProvider / IncludeProvider
ResourceStorage -> TransactionProvider -> BundleProvider
```

#### Tenant-First Design
All persistence operations take a `TenantContext` as the first argument, ensuring data isolation. Every storage backend enforces tenant boundaries at the query level.

#### Composite Storage
The `CompositeStorage` pattern combines backends (e.g., SQLite for CRUD + Elasticsearch for search) behind a single interface. Configured via `HFS_STORAGE_BACKEND`.

## Project Skills

Detailed operational guidance lives in Codex project skills under `.agents/skills/`
(and Claude Code skills under `.claude/skills/`).
Use those skills instead of expanding this always-loaded file.

`.agents/skills` is a byte-for-byte mirror of `.claude/skills`. Edit both together and check with `diff -r .agents/skills .claude/skills` (no output means in sync).

HFS skills:

- `$fhir-developer` - Anthropic FHIR R4 developer skill (resources, cardinality, coding systems, REST/SMART, OperationOutcome). Use for generic FHIR/HL7 API modeling; compose with the HFS-specific skills below for runtime behavior.
- `$run-hfs-server` - HFS server runtime, storage backends, multi-tenancy, compression, and API endpoints.
- `$run-hts-server` - HTS terminology server runtime, env vars, and terminology-seed bootstrap.
- `$run-hfs-and-hts` - Start HFS and HTS together for local dev, wired via HFS_TERMINOLOGY_SERVER/FHIRPATH_TERMINOLOGY_SERVER.
- `$work-with-fhirpath` - FHIRPath CLI, server, expressions, terminology integration, and tests.
- `$work-with-sof` - SQL-on-FHIR, ViewDefinition processing, `sof-cli`, `sof-server`, and parquet output.
- `$work-with-pysof` - Python bindings under `crates/pysof`, maturin setup, API usage, and pysof tests.
- `$test-hfs` - Test strategy, testcontainers, persistence integration tests, and shared test data.
- `$work-with-hts` - Terminology server configuration, APIs, bootstrap sync, and terminology imports.
- `$work-with-auth` - Authentication/authorization, SMART-on-FHIR, JWT/JWKS, scopes, and `HFS_AUTH_*` config.
- `$work-with-audit` - FHIR AuditEvent logging, IHE BALP, audit sinks, and `HFS_AUDIT_*` config.
- `$work-with-subscriptions` - Topic-based Subscriptions engine, channels (rest-hook/websocket/email/messaging), and config.
- `$work-with-cds-hooks` - CDS Hooks protocol types and async service trait for clinical decision support.
- `$work-with-ui` - The `crates/ui` web UI: Askama templates, htmx fragments, `/ui` routes, vendored assets, i18n, and the Rust + Playwright test rings.
- `$hts-api-skill` - Reference for building UI on the HTS HTTP API: routes, content negotiation, OperationOutcome rendering, the `HFS_TERMINOLOGY_SERVER` proxy pattern, and client gaps.
- `$create-issue` - File a GitHub issue describing work to be done, and stop there; never implements the fix.
- `$work-with-book` - Writing and editing pages of the mdBook documentation site under `book/`: format conventions, page template, link check, local build, and tag-only publishing.
- `$bulk-data-export` - FHIR Bulk Data Access `$export` jobs, manifests, output storage, and behavior notes.
- `$bulk-data-submit` - FHIR Bulk Data Submit `$bulk-submit` ingestion, status, OAuth, JWE, and worker settings.
- `$docker-and-release` - Docker image builds and release workflow.

## Environment Setup

### LLD Linker Configuration
Add to `~/.cargo/config.toml`:
```toml
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
```

### Memory-Constrained Builds
```bash
export CARGO_BUILD_JOBS=4
```

### Debugging Tips
- Use `cargo test -- --nocapture` to see println! output
- Enable trace logging: `RUST_LOG=trace cargo run`
- FHIRPath expressions can be tested independently via CLI
- HFS server: `HFS_LOG_LEVEL=debug cargo run --bin hfs`

### Windows and worktree notes
See `CLAUDE.md` -> "Windows and worktree notes" (the only place they live; do not copy them here).

## Commits and CI

- **Never put `[skip ci]` in a commit message, documentation-only commits included.** Five checks are required on `main`: `cargo check (cassandra, R4B)`, `cargo fmt --check`, `cargo clippy (helios-fhir)`, `cargo clippy (workspace)` and `Cassandra schema (single node)` (ADR-aacruzgon-0019, 0021). GitHub runs no workflow for a pull request whose latest commit has `[skip ci]`, so those checks never report and the pull request cannot merge. For the same reason, never add a `paths:` filter to `r4b-compile-check.yml` or `fmt-clippy-cassandra.yml`, and rename a job's `name:` only together with the ruleset. This overrides the `[skip ci]` advice in `CONTRIBUTING.md`, which is upstream's file; the fork only adds its "Dependency Updates" section and leaves that advice as upstream wrote it.
- Workflows run on the self-hosted runners and follow the fork rules in ADR-aacruzgon-0018: the fork guard on every job, and no `pull_request_target` or `workflow_run`. Pin every action to a full commit SHA with its version in a comment, as `.github/workflows/r4b-compile-check.yml` does; the repository setting "Require actions to be pinned to a full-length commit SHA" fails any job that uses a tag or branch (ADR-aacruzgon-0022). A job targets `[self-hosted, hfs-cassandra-light]` (either runner: `sam-7900x` or `acruzdebian`) or `[self-hosted, hfs-cassandra-heavy]` (only `sam-7900x`); a job that may need more than about 9 GB, such as the clippy pair, is heavy (ADR-aacruzgon-0023). Never use the bare `hfs-cassandra` label in a new job.
- **Never start CodeRabbit's Autofix.** Do not tick the Autofix box in a CodeRabbit comment, do not post `@coderabbitai autofix` or its alias `@coderabbitai autopilot`, and never ask CodeRabbit to commit or to finish a pull request: its commits are not signed by the member who asked for them, and they carry a `Co-authored-by: coderabbitai[bot]` trailer or are made with CodeRabbit's own installation credentials. No `.coderabbit.yaml` setting turns Autofix off (see its `finishing_touches` comment).

## Decision records (ADR)

Every design decision made in a session with an AI assistant (Codex, Claude Code or any other) is recorded by the human driving the session, in that person's own ADR log under `docs/Cassandra/`, in the directory `ADR-<github username>/`. That directory holds `ADR-<github username>.md` (rules, entry template and index) and one file per entry, `YYYY-MM-DD/NNNN-<short-slug>.md`, in a folder named for the day the decision was worked on. Current logs: `ADR-aacruzgon/`, `ADR-anastasiabryz/`, `ADR-diya-patel83/`, `ADR-sam-michaels/`, `ADR-sara-lahourpour/`.

- Identify the user's log with `gh api user --jq .login`. If that fails, ask which log is theirs. Do not guess from `git config user.name`.
- Write each entry as a new file from the template in `ADR-<github username>.md`, in the dated folder, and fill every field, including **AI involvement** (what the assistant proposed, what the human changed, what was verified and how). Number entries sequentially across the whole log, not per folder. Add a row linking to the new file to the index table in `ADR-<github username>.md`.
- Write only in the current user's own log directory. Never edit another collaborator's ADR log; a disagreement is a new entry in the disagreeing person's log that references the other.
- A decision that changes `docs/Cassandra/cassandra-discussion.md` or `docs/Cassandra/development-roadmap.md` updates those files in the same commit as the ADR entry.
- Routine work (typo fixes, formatting, a test for existing behaviour) needs no entry. A choice between workable designs, or a change to a table, key, column type, trait surface, consistency level, write path, dependency or benchmark method, or any deviation from the design document, does.
- Draft the entry at the end of the session and show it to the user before committing; the entry is theirs, not the assistant's.

## Important Notes

- Default FHIR version is R4 when no features specified
- **FHIR version feature assumption:** Code MAY assume that at least one FHIR version feature is enabled at compile time, and SHOULD assume R4 is enabled when relying on `FhirVersion::default()` (which is gated on `feature = "R4"`). Avoid adding cfg-ladder fallbacks for the "no version enabled" case - that build target is not supported. Single-version minimal builds (e.g. R4B-only) are supported, but functions that need a default value should require R4 explicitly rather than enumerating versions in `#[cfg]` arms.
- The project follows standard Rust conventions
- `pysof` is excluded from default workspace members - `cargo build` from root skips it
- Server returns appropriate HTTP status codes and FHIR OperationOutcomes for errors
- Minimum supported Rust version: 1.91.1 (edition 2024), from `rust-version` in the root `Cargo.toml`; Extended CI checks it daily. Raise it there if a dependency update needs a newer rustc.
