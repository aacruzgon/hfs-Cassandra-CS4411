#!/usr/bin/env python3
"""Where this job's time went: each finished step and its duration, read from
the Actions API (report only; never fails the job).

GET /repos/{repo}/actions/runs/{run_id}/attempts/{attempt}/jobs, with
GITHUB_TOKEN, which needs `actions: read`
(https://docs.github.com/en/rest/actions/workflow-jobs). The job is found by
its `name:` (the first argument) and this runner's name, among the attempt's
in-progress jobs. Steps that have not finished, this one and any after it,
have no duration yet and are listed as such.

Usage: job-step-timings.py <job name> <out-dir>
Writes <out-dir>/step-timings.json and .md and prints the Markdown. An API
error is reported in the output, and the script still exits 0: a missing
timing table must not fail a job whose real work passed.
"""

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime


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
    try:
        jobs = fetch(url, env["GITHUB_TOKEN"])["jobs"]
    except (urllib.error.URLError, KeyError, ValueError, TimeoutError) as e:
        write(out_dir, {"report": "step-timings", "job": job_name, "error": str(e)},
              f"Step timings unavailable: {e}\n")
        return
    runner = env.get("RUNNER_NAME")
    candidates = [j for j in jobs if j["name"] == job_name and j["status"] == "in_progress"]
    mine = [j for j in candidates if j.get("runner_name") == runner] or candidates
    if not mine:
        write(out_dir, {"report": "step-timings", "job": job_name, "error": "job not found"},
              f"Step timings unavailable: no in-progress job named `{job_name}` in this run.\n")
        return
    job = mine[0]

    started = iso(job.get("started_at"))
    rows = []
    for step in job["steps"]:
        begin, end = iso(step.get("started_at")), iso(step.get("completed_at"))
        seconds = int((end - begin).total_seconds()) if begin and end else None
        rows.append(
            {
                "number": step["number"],
                "name": step["name"],
                "conclusion": step.get("conclusion") or step["status"],
                "seconds": seconds,
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
        f"{total} s across its finished steps. The API gives whole seconds, so short steps read 0 s.",
        "",
        "| # | Step | Result | Seconds | Share |",
        "| ---: | --- | --- | ---: | ---: |",
    ]
    for r in rows:
        if r["seconds"] is None:
            lines.append(f"| {r['number']} | {r['name']} | {r['conclusion']} | not finished | |")
        else:
            share = f"{100 * r['seconds'] / total:.0f}%" if total else ""
            lines.append(f"| {r['number']} | {r['name']} | {r['conclusion']} | {r['seconds']} | {share} |")
    write(out_dir, report, "\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
