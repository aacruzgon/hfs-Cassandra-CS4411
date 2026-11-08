# Disabled workflows

These inherited workflows are outside the Cassandra project scope and are
preserved here for reference. GitHub Actions only loads workflows from
`.github/workflows/`, so the YAML files in this directory do not run.

Five workflows are active. Four run on the team's self-hosted runners
(ADR-aacruzgon-0018, ADR-sam-michaels-0001, ADR-aacruzgon-0023):
`r4b-compile-check.yml` (roadmap item 0.4), `fmt-clippy-cassandra.yml` (0.5),
`upstream-canary.yml` (0.11) and `runner-docker-prune.yml`. Light jobs use
`[self-hosted, hfs-cassandra-light]`, the two clippy jobs
`[self-hosted, hfs-cassandra-heavy]`, and the Rust jobs keep a bounded cargo
target directory on each machine instead of a cache (ADR-sam-michaels-0002).

The standalone `.github/workflows/coverage.yml` workflow runs on standard
GitHub-hosted Linux runners. It measures the default Rust workspace
build (R4 / SQLite), excluding `pysof`, generated FHIR models, and SMTP
integration tests. Optional backend integration coverage is not yet enabled.
It uploads reports to Codecov using the `CODECOV_TOKEN` repository secret
(tokenless for fork PRs); no remote Docker host is needed. Install the
[Codecov GitHub app](https://github.com/apps/codecov) for this repository and
connect it at [Codecov](https://app.codecov.io/gh/aacruzgon/hfs-Cassandra-CS4411) to finish
the service setup. Coverage runs on pushes to `main`, pull requests targeting
`main`, and manual dispatches.

`ci.yml` and `fhir-benchmark.yml` were
archived as well: every job in them targets `self-hosted` runner labels, and
`ci.yml` also needs a remote Docker host (`DOCKER_HOST` secret) for
testcontainers. When they were archived this fork had no self-hosted runner, so
their jobs queued indefinitely. Since #16 it has `sam-7900x`
(ADR-sam-michaels-0001), and since 2026-11-04 also `acruzdebian`
(ADR-aacruzgon-0023), but `ci.yml` still lacks the remote Docker host. Several
`ci.yml` jobs target plain `[self-hosted]` or `[self-hosted, Linux]`, which both
runners match, so a restored copy must be retargeted to
`[self-hosted, hfs-cassandra-light]` or `[self-hosted, hfs-cassandra-heavy]` and
given the fork guard from ADR-aacruzgon-0018 first. Follow-up CI, including
adapting the benchmark to Cassandra/PostgreSQL, runs on the self-hosted runners
(ADR-aacruzgon-0018).

`cassandra-integration.yml` is intentionally empty. Implement its triggers and
jobs before moving it into `.github/workflows/`; an empty active workflow would
fail validation.

To restore an inherited workflow, move its YAML file into `.github/workflows/`
and restore any necessary caller jobs or dependencies. `ci.yml` is upstream's
copy as of the 2026-11-08 sync, so it still calls `hts.yml`,
`hts-ig-conformance.yml`, `release-build.yml` and `deploy-server.yml`, all of
which are in this directory: restore those too, or remove the calls.
