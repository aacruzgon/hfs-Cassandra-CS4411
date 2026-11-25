# Disabled workflows

These inherited workflows are outside the Cassandra project scope and are
preserved here for reference. GitHub Actions only loads workflows from
`.github/workflows/`, so the YAML files in this directory do not run.

Ten workflows are active. Nine run on the team's self-hosted runners
(ADR-aacruzgon-0018, ADR-sam-michaels-0001, ADR-aacruzgon-0023):
`r4b-compile-check.yml` (roadmap item 0.4), `fmt-clippy-cassandra.yml` (0.5),
`upstream-canary.yml` (0.11), `runner-docker-prune.yml`,
`cassandra-coverage.yml`, `fhir-versions.yml`, `benchmark-datasets.yml` (0.7),
`cassandra-mutants.yml` and `cassandra-flaky.yml` (0.13). Light jobs use
`[self-hosted, hfs-cassandra-light]`; the two clippy jobs,
`fhir-versions.yml`'s all-versions test job and the flakiness job
`[self-hosted, hfs-cassandra-medium]`; and the mutation-testing job, which runs
only on a pull request labelled `mutants`, `[self-hosted, hfs-cassandra-heavy]`,
on `acruzdebian` alone (ADR-aacruzgon-0082). The Rust jobs keep a bounded cargo
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
archived as well: apart from one `ubuntu-latest` job in each (`publish-report`
in `ci.yml`, `compare` in `fhir-benchmark.yml`), the jobs in them that name a
runner target `self-hosted` labels, and
`ci.yml` also needs a remote Docker host (`DOCKER_HOST` secret) for
testcontainers. When they were archived this fork had no self-hosted runner, so
those jobs queued indefinitely. Since #16 it has `sam-7900x`
(ADR-sam-michaels-0001), and since 2026-11-04 also `acruzdebian`
(ADR-aacruzgon-0023), but `ci.yml` still lacks the remote Docker host. Several
`ci.yml` jobs target plain `[self-hosted]` or `[self-hosted, Linux]`, which both
runners match, so a restored copy must be retargeted to
`[self-hosted, hfs-cassandra-light]`, `[self-hosted, hfs-cassandra-medium]` or
`[self-hosted, hfs-cassandra-heavy]` (ADR-aacruzgon-0082) and given the fork
guard from ADR-aacruzgon-0018 first. Follow-up CI, including
adapting the benchmark to Cassandra/PostgreSQL, runs on the self-hosted runners
(ADR-aacruzgon-0018).

`docker-lint.yml` came with the 2026-11-24 upstream sync (HeliosSoftware/hfs#2591)
and was moved here before that sync merged, as every workflow a sync adds is
(ADR-aacruzgon-0022). It runs on GitHub-hosted `ubuntu-latest` on any pull
request that changes `Dockerfile`, `.dockerignore` or `.hadolint.yaml`, and
uses `actions/checkout` by tag, so the repository's SHA-pinning setting would
fail it before its first step.

`cassandra-integration.yml` is intentionally empty. Implement its triggers and
jobs before moving it into `.github/workflows/`; an empty active workflow would
fail validation.

To restore an inherited workflow, move its YAML file into `.github/workflows/`
and restore any necessary caller jobs or dependencies. `ci.yml` is upstream's
copy as of the 2026-11-08 sync, so it still calls `hts.yml`,
`hts-ig-conformance.yml`, `release-build.yml` and `deploy-server.yml`, all of
which are in this directory: restore those too, or remove the calls.
