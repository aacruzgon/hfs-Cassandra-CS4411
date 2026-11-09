# Demonstration script

`demo.py` is the demonstration from `docs/Cassandra/development-roadmap.md`: one HTTP session against a running HFS server (rows 0.9 and 8.6, Step 8). It needs Python 3.9 or later and nothing else, and it talks only to the URL it is given.

It covers create, read, vread, an update with an `If-Match` conflict, a batch bundle, a transaction bundle, token, date and URI searches with paging through `next` links, a rejected `:exact`, a delete followed by 410, and isolation between two tenants.

The script does not start anything. Start the database and HFS first, then run it. Run the commands below as a shell script, not in an interactive shell, because the port check ends with `exit`.

## Against PostgreSQL

```bash
docker compose -f docker/postgres/docker-compose.yml up -d --wait
cargo build -p helios-hfs --bin hfs --features postgres

if curl -s -m 2 -o /dev/null http://127.0.0.1:58080/metadata; then
  echo "something already answers on port 58080: stop it first" >&2
  exit 1
fi

HFS_STORAGE_BACKEND=postgres \
HFS_DATABASE_URL=postgresql://helios:helios@127.0.0.1:55432/helios \
HFS_SERVER_PORT=58080 HFS_BASE_URL=http://127.0.0.1:58080 \
./target/debug/hfs > target/demo-hfs.log 2>&1 &
HFS_PID=$!

python3 docs/Cassandra/demo/demo.py --backend postgres --wait 120 --server-log target/demo-hfs.log
kill "$HFS_PID"
```

## Against Cassandra

Apply the schema once, as `docs/Cassandra/schema/README.md` says; HFS only checks that the ten tables exist and never creates them.

```bash
cargo build -p helios-hfs --bin hfs --features cassandra

if curl -s -m 2 -o /dev/null http://127.0.0.1:58080/metadata; then
  echo "something already answers on port 58080: stop it first" >&2
  exit 1
fi

HFS_STORAGE_BACKEND=cassandra \
HFS_CASSANDRA_CONTACT_POINTS=127.0.0.1:59042 \
HFS_SERVER_PORT=58080 HFS_BASE_URL=http://127.0.0.1:58080 \
./target/debug/hfs > target/demo-hfs.log 2>&1 &
HFS_PID=$!

python3 docs/Cassandra/demo/demo.py --backend cassandra --wait 120 --server-log target/demo-hfs.log
kill "$HFS_PID"
```

Search and paging are later roadmap steps (6 and 6a to 6d), so until they land the search steps fail on Cassandra. Row 8.6 is the first point at which the whole script has to pass there.

## Options

| Option | Meaning |
| --- | --- |
| `--backend postgres\|cassandra` | Required. Which backend's documented answers to expect. It does not verify the server: see "Confirming the backend". |
| `--base-url URL` | The HFS URL. Default `http://127.0.0.1:58080`. |
| `--wait SECONDS` | Retry `GET /metadata` for this long before starting. Default: no wait. |
| `--timeout SECONDS` | One request's timeout. Default 60. |
| `--report FILE` | Also write JSON Lines: one object per step, then a summary object with `"summary": true`. |
| `--server-log FILE` | HFS's log. Adds a step that checks the backend, URL and port it says HFS was started with. |

Set `HFS_BASE_URL` to the same URL as `--base-url`. HFS builds `next` links from it, and the script fails a paging step whose link points anywhere else, because it promises to talk only to the URL it was given.

Port 58080 is used by nothing in `.github/workflows/` or `docker/`.

## Confirming the backend

`--backend` only chooses which answers to expect. `/metadata` and `/health` do not name the storage backend, so a SQLite HFS passes every step of `--backend postgres`, and an HFS left running on the port answers in place of the one you meant to start.

`--server-log` covers the first gap as far as a log can. It reads the last `Starting Helios FHIR Server` line of HFS's log, which HFS writes at `info`, the default level, and adds a step that requires `storage_backend` to equal `--backend`, `public_base_url` to equal `--base-url`, and `port` to equal the port in `--base-url`. The report's summary then has `"backend_confirmed": true`. Without the option it is `false`, and the run says so. Because the ports must match, the option is for a server the script reaches directly: through a proxy on another port, the step fails and says so.

The log says what HFS was started with, not who answers on the port. HFS writes that line before it connects to its database and before it binds, so `backend_confirmed` means only that the log matches this run.

Two checks cover the second gap, and neither is the script's:

- **Before starting HFS, refuse a port that already answers,** as the commands above do. Otherwise an old HFS answers while the new one is still starting, and the script can finish with every step passed and `"backend_confirmed": true` before the new one tries to bind.
- **Once the script ends, check that the HFS you started is still running.** An HFS that cannot bind its port writes that same line and then exits 1, while whatever holds the port keeps answering. `kill "$HFS_PID"` in the commands above fails in that case; a CI job must fail on it.

Row 8.6's job needs all four: the refusal of a port that already answers, a new log file for each run, `--server-log` with `"backend_confirmed": true` in the summary, and the process check. Only the database shows directly which backend stored a run. The report's summary has the `run_id`, and the run's tenants are `demo-<run id>-a` and `-b`, so the job may also count that run's rows there.

## Output

Each step prints its name, the request, the expected and actual status, the time it took, and where the expected status is documented, followed by any problem. The last line counts the steps.

The exit status is 0 when every step passed, 1 when any failed, and 2 when the script could not start: bad arguments or, with `--wait`, no server answering in time. Without `--wait`, a server that does not answer is a run of failed steps, so 1.

Each section makes its own resources, so a failure in one does not stop the others. They do share tenant A, which is why only the search section's patients have a gender or a birth date: `Patient?gender=male` carries no run marker. Inside a section, a step whose result later steps need stops that section, and the run records this as a failed step.

Every run makes new tenant names (`demo-<run id>-a` and `-b`) and new resource ids and search values, so it depends on nothing an earlier run left, and two runs never clash.

## Where the backends differ

Three steps have a different documented answer on each backend. The script states both and checks the one for `--backend`.

| Step | PostgreSQL | Cassandra | Why |
| --- | --- | --- | --- |
| transaction bundle | 200, `transaction-response` | 501, `not-supported` | Cassandra has no atomic transaction across partitions, so the REST layer refuses the bundle before applying any entry: `schema/coverage.md` section 6, "Transaction Bundle"; `crates/rest/src/handlers/batch.rs`, `AtomicityUnsupported`. R4 says a server without transactions SHOULD answer 400 ([http.html#transaction](https://hl7.org/fhir/R4/http.html#transaction)); HFS answers 501, and the script checks what HFS documents. |
| reading the transaction's entry back | 200, it was applied | 404, nothing was applied | The refusal comes before any entry is applied (`schema/coverage.md` section 6), so a 501 that had applied the entry fails here. |
| `Patient?family:exact=Smith` | 200, whole-string match | 400 | R4 defines `:exact` for string parameters only. PostgreSQL implements it. Cassandra has no string search, so its type check refuses the parameter: `schema/coverage.md` section 6. |

`:exact` on a token parameter (`Patient?gender:exact=female`) is a 400 on both, from the REST layer, before any backend runs.
