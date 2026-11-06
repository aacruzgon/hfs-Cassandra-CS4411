#!/usr/bin/env python3
"""Where this job's time went: each finished step and its duration, read from
the Actions API (report only; never fails the job).

GET /repos/{repo}/actions/runs/{run_id}/attempts/{attempt}/jobs, with
GITHUB_TOKEN, which needs `actions: read`
(https://docs.github.com/en/rest/actions/workflow-jobs). The job is found by
its `name:` (the first argument) and this runner's name, among the attempt's
in-progress jobs. Steps that have not finished, this one and any after it,
have no duration yet and are listed as such.

The API reports a step's progress late: in run 42261862863 the build step that
had just ended still read "in_progress", and this step itself still read
"pending". So a step before this one (THIS_STEP, by name) that has a start but
no completion is timed from its start to now, the moment this step reads the
API, and marked as an estimate; it ended at most that long ago. Waiting for
the API instead would add tens of seconds to every job.

Usage: job-step-timings.py <job name> <out-dir>
Writes <out-dir>/step-timings.json and .md and prints the Markdown. An API
error is reported in the output, and the script still exits 0: a missing
timing table must not fail a job whose real work passed.
"""

import http.client
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone


THIS_STEP = os.environ.get("STEP_TIMINGS_STEP", "Report where this job's time went")


def iso(ts):
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ") if ts else None


def fetch(url, token):
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "hfs-Cassandra-CS4411-ci-step-timings",
        },
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def write(out_dir, report, md):
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "step-timings.json"), "w") as f:
        json.dump(report, f, indent=2)
        f.write("\n")
    with open(os.path.join(out_dir, "step-timings.md"), "w") as f:
        f.write(md)
    print(md)


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: job-step-timings.py <job name> <out-dir>")
    job_name, out_dir = sys.argv[1], sys.argv[2]
    env = os.environ
    api = env.get("GITHUB_API_URL", "https://api.github.com")
    url = (
        f"{api}/repos/{env['GITHUB_REPOSITORY']}/actions/runs/{env['GITHUB_RUN_ID']}"
        f"/attempts/{env.get('GITHUB_RUN_ATTEMPT', '1')}/jobs?per_page=100"
    )
    runner = env.get("RUNNER_NAME")
    try:
        jobs = fetch(url, env["GITHUB_TOKEN"])["jobs"]
    # OSError covers URLError, socket.timeout (not a TimeoutError before
    # Python 3.10) and a reset connection; HTTPException an incomplete read.
    except (OSError, KeyError, ValueError, http.client.HTTPException) as e:
        write(out_dir, {"report": "step-timings", "job": job_name, "error": str(e)},
              f"Step timings unavailable: {e}\n")
        return
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    candidates = [j for j in jobs if j["name"] == job_name and j["status"] == "in_progress"]
    mine = [j for j in candidates if j.get("runner_name") == runner] or candidates
    if not mine:
        write(out_dir, {"report": "step-timings", "job": job_name, "error": "job not found"},
              f"Step timings unavailable: no in-progress job named `{job_name}` in this run.\n")
        return
    job = mine[0]
    names = [st["name"] for st in job["steps"]]
    this_index = names.index(THIS_STEP) if THIS_STEP in names else len(names)

    started = iso(job.get("started_at"))
    rows = []
    for index, step in enumerate(job["steps"]):
        begin, end = iso(step.get("started_at")), iso(step.get("completed_at"))
        estimated = False
        if begin and end:
            seconds = int((end - begin).total_seconds())
        elif begin and index < this_index:
            # Finished before this step began; the API has not caught up.
            seconds, estimated = max(0, int((now - begin).total_seconds())), True
        else:
            seconds = None
        rows.append(
            {
                "number": step["number"],
                "name": step["name"],
                "conclusion": step.get("conclusion") or ("success (API not caught up)" if estimated else step["status"]),
                "seconds": seconds,
                "estimated": estimated,
            }
        )
    finished = [r for r in rows if r["seconds"] is not None]
    total = sum(r["seconds"] for r in finished)
    report = {
        "report": "step-timings",
        "job": job_name,
        "job_id": job["id"],
        "runner": job.get("runner_name"),
        "job_started_at": job.get("started_at"),
        "finished_steps_s": total,

        "steps": rows,
    }

    lines = [
        f"Job `{job_name}` on `{job.get('runner_name')}`, job id {job['id']}: "
        f"{total} s across its finished steps. The API gives whole seconds, so short steps read 0 s."
        + (" A time marked ≤ is an estimate: the API had not yet marked that step finished, so it is "
           "measured to the moment this report read the API." if any(r["estimated"] for r in rows) else ""),
        "",
        "| # | Step | Result | Seconds | Share |",
        "| ---: | --- | --- | ---: | ---: |",
    ]
    for r in rows:
        if r["seconds"] is None:
            lines.append(f"| {r['number']} | {r['name']} | {r['conclusion']} | not finished | |")
        else:
            share = f"{100 * r['seconds'] / total:.0f}%" if total else ""
            shown = f"≤ {r['seconds']}" if r["estimated"] else str(r["seconds"])
            lines.append(f"| {r['number']} | {r['name']} | {r['conclusion']} | {shown} | {share} |")
    write(out_dir, report, "\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
