#!/usr/bin/env python3
"""Tests for bench.py's analysis. No Docker, k6 or server needed.

    python3 docs/Cassandra/benchmark/test_bench.py
"""

import argparse
import csv
import gzip
import json
import math
import os
import signal
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import bench


def write_k6_csv(path: Path, rows) -> None:
    header = "metric_name,timestamp,metric_value,check,error,error_code,group,method,name,proto,scenario,status,subproto,tls_version,url,extra_tags\n"
    with gzip.open(path, "wt", encoding="utf-8") as sink:
        sink.write(header)
        for name, ts, value, status in rows:
            sink.write(f"{name},{ts},{value},,,,,,create,,,{status},,,,\n")


class PercentileTests(unittest.TestCase):
    def test_nearest_rank(self):
        data = [float(n) for n in range(1, 101)]
        self.assertEqual(bench.percentile(data, 0.50), 50.0)
        self.assertEqual(bench.percentile(data, 0.95), 95.0)
        self.assertEqual(bench.percentile(data, 0.99), 99.0)

    def test_one_value(self):
        self.assertEqual(bench.percentile([7.0], 0.99), 7.0)


class StatusClassTests(unittest.TestCase):
    def test_409_is_never_a_5xx_or_other_4xx(self):
        self.assertEqual(bench.status_class("409"), "s409")
        self.assertEqual(bench.status_class("412"), "s412")
        self.assertEqual(bench.status_class("404"), "s4xx_other")
        self.assertEqual(bench.status_class("500"), "s5xx_other")
        self.assertEqual(bench.status_class("503"), "s5xx_other")

    def test_501_is_its_own_class(self):
        self.assertEqual(bench.status_class("501"), "s501")

    def test_network_errors_are_other(self):
        self.assertEqual(bench.status_class("0"), "s_other")
        self.assertEqual(bench.status_class("302"), "s_other")

    def test_2xx(self):
        self.assertEqual(bench.status_class("200"), "s2xx")
        self.assertEqual(bench.status_class("201"), "s2xx")


class SteadyTests(unittest.TestCase):
    def test_flat_is_zero(self):
        self.assertEqual(bench.steady_change_pct([1, 100, 100, 100, 100, 100, 100, 3]), 0.0)

    def test_decline_is_negative(self):
        change = bench.steady_change_pct([1, 100, 100, 50, 50, 25, 25, 3])
        self.assertLess(change, -50.0)

    def test_too_few_bins(self):
        self.assertIsNone(bench.steady_change_pct([1, 100, 3]))


class AnalyseTests(unittest.TestCase):
    def test_counts_bins_and_requests(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            k6 = root / "k6.csv.gz"
            rows = [
                ("http_req_duration", 1000, 10.0, 200),
                ("http_reqs", 1000, 1, 200),
                ("http_req_duration", 2000, 20.0, 201),
                ("http_req_duration", 11000, 30.0, 409),
                ("http_req_duration", 12000, 40.0, 500),
                ("http_req_duration", 21000, 50.0, 0),
                ("dropped_iterations", 21500, 2, ""),
            ]
            write_k6_csv(k6, rows)
            metrics = bench.analyse(k6, root / "requests.csv.gz", root / "bins.csv", 30.0, 10)
            self.assertEqual(metrics["completed"], 5)
            self.assertEqual(metrics["s2xx"], 2)
            self.assertEqual(metrics["s409"], 1)
            self.assertEqual(metrics["s5xx_other"], 1)
            self.assertEqual(metrics["s_other"], 1)
            self.assertEqual(metrics["dropped_iterations"], 2)
            self.assertAlmostEqual(metrics["ok_rps"], 2 / 30)
            with gzip.open(root / "requests.csv.gz", "rt") as stream:
                requests = list(csv.DictReader(stream))
            self.assertEqual(len(requests), 5)
            self.assertEqual(requests[2]["status"], "409")
            with (root / "bins.csv").open() as stream:
                bins = list(csv.DictReader(stream))
            self.assertEqual([b["bin_start_s"] for b in bins], ["0", "10", "20"])
            self.assertEqual(bins[1]["s409"], "1")
            self.assertEqual(bins[2]["dropped_iterations"], "2")


    def test_a_stall_shows_as_zero_bins_and_a_decline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            k6 = root / "k6.csv.gz"
            rows = [("http_req_duration", 1000 * s, 10.0, 200) for s in range(0, 40)]
            rows += [("http_req_duration", 1000 * s, 10.0, 200) for s in range(80, 90)]
            write_k6_csv(k6, rows)
            metrics = bench.analyse(k6, root / "r.csv.gz", root / "bins.csv", 90.0, 10)
            with (root / "bins.csv").open() as stream:
                bins = list(csv.DictReader(stream))
            self.assertEqual(len(bins), 9)
            self.assertEqual(bins[5]["completed"], "0")
            self.assertEqual(bins[5]["p50_ms"], "")
            self.assertLess(metrics["steady_change_pct"], -50.0)

    def test_errors_count_as_a_decline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            k6 = root / "k6.csv.gz"
            rows = [("http_req_duration", 1000 * s, 10.0, 200 if s < 45 else 503) for s in range(0, 90)]
            write_k6_csv(k6, rows)
            metrics = bench.analyse(k6, root / "r.csv.gz", root / "bins.csv", 90.0, 10)
            self.assertLess(metrics["steady_change_pct"], -50.0)


class DatasetCheckTests(unittest.TestCase):
    NAME = "patient-observation-10000.ndjson"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.path = self.root / self.NAME
        self.path.write_bytes(b'{"resourceType":"Patient","id":"a"}\n')
        self.expected = bench.sha256_file(self.path)
        self.sums = self.root / "SHA256SUMS"
        self.sums.write_text(f"{self.expected}  {self.NAME}\n", encoding="utf-8")

    def tearDown(self):
        self.tmp.cleanup()

    def test_the_published_file_passes(self):
        self.assertEqual(bench.verify_dataset(self.path, self.sums), self.expected)

    def test_a_one_byte_edit_is_refused_and_named(self):
        self.path.write_bytes(b'{"resourceType":"Patient","id":"b"}\n')
        with self.assertRaises(bench.Failure) as caught:
            bench.verify_dataset(self.path, self.sums)
        message = str(caught.exception)
        self.assertIn(self.NAME, message)
        self.assertIn(self.expected, message)
        self.assertIn(bench.sha256_file(self.path), message)

    def test_a_missing_file_is_refused(self):
        self.path.unlink()
        with self.assertRaises(bench.Failure) as caught:
            bench.verify_dataset(self.path, self.sums)
        self.assertIn(self.expected, str(caught.exception))
        self.assertIn("missing", str(caught.exception))

    def test_a_missing_line_is_refused(self):
        self.sums.write_text("00  other.ndjson\n", encoding="utf-8")
        with self.assertRaises(bench.Failure) as caught:
            bench.verify_dataset(self.path, self.sums)
        self.assertIn("no line", str(caught.exception))
        self.assertIn(self.expected, str(caught.exception))

    def test_a_missing_checksum_file_is_refused(self):
        self.sums.unlink()
        with self.assertRaises(bench.Failure) as caught:
            bench.verify_dataset(self.path, self.sums)
        self.assertIn("does not exist", str(caught.exception))


class HostLoadTests(unittest.TestCase):
    def test_own_descendants_are_excluded_and_small_processes_ignored(self):
        text = "100 1 90.0 /usr/bin/python3\n200 100 80.0 k6\n300 200 70.0 hfs\n400 1 50.0 rustc\n500 1 1.0 idle thing\n"
        foreign = bench.foreign_processes(bench.parse_ps(text), 100)
        self.assertEqual([item["pid"] for item in foreign], [400])
        self.assertEqual(foreign[0]["command"], "rustc")

    def test_quiet(self):
        # A high load average alone is the harness's own previous run.
        quiet = {"load_1m": 40.0, "cpus": 8, "foreign_cpu_pct": bench.QUIET_FOREIGN_CPU_PCT}
        self.assertTrue(bench.host_is_quiet(quiet))
        self.assertFalse(bench.host_is_quiet(dict(quiet, foreign_cpu_pct=bench.QUIET_FOREIGN_CPU_PCT + 1)))


class DockerBudgetWaitTests(unittest.TestCase):
    def test_a_full_budget_stops_at_the_deadline_instead_of_waiting_on(self):
        args = argparse.Namespace(profile="3g", project=None, db_port=None, docker_budget_gib=8.3, deadline=time.time() - 1)
        backend = bench.Postgres(args)
        full = subprocess.CompletedProcess([], 0, stdout="other\t1GiB / 8GiB\n", stderr="")
        with mock.patch.object(bench, "run_cmd", return_value=full):
            with self.assertRaises(bench.BudgetExhausted):
                backend._wait_for_budget()


class ResetDefaultTests(unittest.TestCase):
    COMMANDS = (
        ["run", "--backend", "postgres", "--dataset-dir", "d", "--out", "o"],
        ["calibrate", "--dataset-dir", "d", "--out", "o"],
        ["overnight", "--dataset-dir", "d", "--hours", "1"],
    )

    def test_snapshot_is_the_default_for_every_matrix_command(self):
        for command in self.COMMANDS:
            self.assertEqual(bench.build_parser().parse_args(command).reset, "snapshot", command[0])

    def test_reload_stays_available(self):
        for command in self.COMMANDS:
            self.assertEqual(bench.build_parser().parse_args(command + ["--reset", "reload"]).reset, "reload", command[0])


class ResumeArgumentsTests(unittest.TestCase):
    def test_keeps_options_and_drops_what_the_resume_sets(self):
        argv = ["--hours", "8", "--runs", "1", "--out=/x", "--resume", "--dataset-dir", "d", "--out", "/y"]
        self.assertEqual(bench.resume_arguments(argv), ["--runs", "1", "--dataset-dir", "d"])


class OvernightCountsTests(unittest.TestCase):
    def test_a_declined_cell_counts_as_done_and_left_is_what_remains(self):
        part = {"sizes": [10000], "ops": ["create", "token"], "concurrency": [8], "runs": 3}
        records = [
            {"size": 10000, "op": "create", "concurrency": 8, "status": "ok"},
            {"size": 10000, "op": "create", "concurrency": 8, "status": "failed"},
            {"size": 10000, "op": "token", "concurrency": 8, "status": "not_implemented", "host_busy": True},
        ]
        counts = bench.part_counts(records, part)
        self.assertEqual(counts["ok"], 1)
        self.assertEqual(counts["declined_cells"], 1)
        self.assertEqual(counts["failed"], 1)
        self.assertEqual(counts["busy_host"], 1)
        self.assertEqual(counts["left"], 2)
        self.assertEqual(counts["total"], 6)


class HelperTests(unittest.TestCase):
    def test_calibrated_rate_is_80_percent_floor(self):
        self.assertEqual(bench.calibrated_rate(1000.0), 800)
        self.assertEqual(bench.calibrated_rate(0.9), 1)

    def test_docker_limits(self):
        lines = ["hfs-cassandra-cassandra-1\t1.821GiB / 3GiB", "x\t512MiB / 1.5GiB"]
        self.assertAlmostEqual(bench.docker_limits_gib(lines), 4.5)

    def test_search_values_are_deterministic_and_encoded(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "d.ndjson"
            observation = {
                "resourceType": "Observation",
                "id": "a",
                "code": {"coding": [{"system": "http://loinc.org", "code": "1-1"}]},
                "effectiveDateTime": "2020-12-10T00:00:00Z",
                "meta": {"profile": ["http://example.org/p"]},
            }
            path.write_text(
                json.dumps({"resourceType": "Patient", "id": "p"}) + "\n" + json.dumps(observation) + "\n",
                encoding="utf-8",
            )
            ids, queries = bench.build_search_params(path)
            self.assertEqual(ids, ["Patient/p", "Observation/a"])
            self.assertEqual(queries["token"], ["Observation?code=http%3A%2F%2Floinc.org%7C1-1&_count=20"])
            self.assertEqual(queries["date"], ["Observation?date=ge2020-12-01&date=lt2021-01-01&_count=20"])
            self.assertEqual(queries["uri"], ["Observation?_profile=http%3A%2F%2Fexample.org%2Fp&_count=20"])


class BudgetTests(unittest.TestCase):
    """Overnight never starts a run that can't finish before the deadline."""

    @staticmethod
    def plan(left_seconds, **overrides):
        values = dict(deadline=time.time() + left_seconds, warmup=60, duration=120, no_reset=False,
                      runs=3, mode="open", run_seconds={})
        values.update(overrides)
        return argparse.Namespace(**values)

    @staticmethod
    def parts():
        args = argparse.Namespace(project=None, db_port=None, profile="3g", baseline="on")
        return bench.Cassandra(args), argparse.Namespace(size=10000, sha256="a" * 64)

    def test_lwt_is_not_started_on_the_timing_of_a_short_open_loop_run(self):
        backend, dataset = self.parts()
        lwt = self.plan(600, mode="closed", no_reset=True, duration=600, warmup=60)
        # The earlier part of the same backend and size timed a run of about a minute.
        open_loop = self.plan(600)
        bench.note_run_seconds(open_loop, backend, dataset, 60.0)
        lwt.run_seconds = open_loop.run_seconds
        self.assertIn("needs about 2130 s", bench.out_of_budget(lwt, backend, dataset, runs_left=3, fresh=True))

    def test_lwt_starts_when_all_three_runs_fit(self):
        backend, dataset = self.parts()
        lwt = self.plan(2200, mode="closed", no_reset=True, duration=600, warmup=60)
        self.assertIsNone(bench.out_of_budget(lwt, backend, dataset, runs_left=3, fresh=True))

    def test_a_later_no_reset_run_is_checked_too(self):
        backend, dataset = self.parts()
        lwt = self.plan(300, mode="closed", no_reset=True, duration=600, warmup=60)
        self.assertIsNotNone(bench.out_of_budget(lwt, backend, dataset, runs_left=1, fresh=False))

    def test_the_slowest_timed_run_of_a_kind_is_the_estimate(self):
        backend, dataset = self.parts()
        plan = self.plan(150)
        bench.note_run_seconds(plan, backend, dataset, 200.0)
        bench.note_run_seconds(plan, backend, dataset, 100.0)
        self.assertIsNotNone(bench.out_of_budget(plan, backend, dataset))
        self.assertIsNone(bench.out_of_budget(self.plan(250, run_seconds=plan.run_seconds), backend, dataset))

    def test_no_deadline_means_no_limit(self):
        backend, dataset = self.parts()
        self.assertIsNone(bench.out_of_budget(argparse.Namespace(), backend, dataset))


class HostLoadVmTests(unittest.TestCase):
    PS = {1: (0, 250.0, "/Applications/OrbStack.app/Contents/Frameworks/OrbStack Helper.app/Contents/MacOS/OrbStack Helper"),
          2: (0, 30.0, "/System/Library/WindowServer"), 3: (0, 90.0, "/usr/bin/rustc")}

    def test_the_docker_vm_is_not_foreign(self):
        names = [item["command"] for item in bench.foreign_processes(self.PS, 99)]
        self.assertEqual(names, ["/usr/bin/rustc", "/System/Library/WindowServer"])

    def test_own_database_on_linux_is_not_foreign(self):
        processes = {10: (1, 300.0, "java"), 11: (10, 50.0, "java"), 12: (1, 80.0, "rustc")}
        self.assertEqual([i["command"] for i in bench.foreign_processes(processes, 99, [10])], ["rustc"])

    def test_other_containers_count_and_our_own_do_not(self):
        lines = ["hfsbench-cassandra-cassandra-1\t1GiB / 3GiB\t280.5%", "someone-else-1\t1GiB / 3GiB\t120%", "idle-1\t0B / 1GiB\t0.1%"]
        self.assertEqual([i["cpu_pct"] for i in bench.other_containers_cpu(lines, "hfsbench-cassandra")], [120.0])


class SnapshotNameTests(unittest.TestCase):
    def test_two_backends_in_one_project_get_two_names(self):
        args = argparse.Namespace(project="mine", db_port=None, profile="3g", baseline="on")
        dataset = argparse.Namespace(size=10000, sha256="a" * 64)
        self.assertNotEqual(bench.Postgres(args).snapshot_volume(dataset, "b" * 40),
                            bench.Cassandra(args).snapshot_volume(dataset, "b" * 40))


class PortTests(unittest.TestCase):
    def test_a_held_port_is_refused_and_a_free_one_is_not(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            port = listener.getsockname()[1]
            with self.assertRaises(bench.Failure) as refused:
                bench.require_free_port(port)
            self.assertIn(f"port {port} is held", str(refused.exception))
        bench.require_free_port(port)


class ReadingTests(unittest.TestCase):
    def analyse(self, rows):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_k6_csv(root / "k6.csv.gz", rows)
            return bench.analyse(root / "k6.csv.gz", root / "requests.csv.gz", root / "bins.csv", 10.0, 10)

    def test_percentiles_over_2xx_leave_fast_failures_out(self):
        rows = [("http_req_duration", 1000 + n, 100.0, "201") for n in range(50)]
        rows += [("http_req_duration", 2000 + n, 1.0, "503") for n in range(50)]
        metrics = self.analyse(rows)
        self.assertEqual(metrics["p50_ms"], 1.0)
        self.assertEqual(metrics["p50_ok_ms"], 100.0)
        self.assertEqual(metrics["p95_ok_ms"], 100.0)

    def test_a_run_with_no_2xx_has_no_2xx_percentile(self):
        self.assertTrue(math.isnan(self.analyse([("http_req_duration", 1000, 1.0, "503")])["p95_ok_ms"]))

    def test_saturation_is_above_one_percent_of_the_schedule(self):
        self.assertEqual(bench.saturation(990, 10)["latency_reading"], "normal")
        over = bench.saturation(980, 20)
        self.assertEqual(over["latency_reading"], "saturated")
        self.assertEqual(over["offered"], 1000)
        self.assertAlmostEqual(over["drop_pct"], 2.0)

    def test_an_analysed_run_carries_its_reading(self):
        rows = [("http_req_duration", 1000 + n, 5.0, "200") for n in range(90)]
        rows.append(("dropped_iterations", 1100, 10.0, ""))
        self.assertEqual(self.analyse(rows)["latency_reading"], "saturated")


class MinorTests(unittest.TestCase):
    def test_a_starting_container_does_not_break_the_limit_sum(self):
        self.assertAlmostEqual(bench.docker_limits_gib(["x\t-- / --", "y\t1GiB / 3GiB"]), 3.0)

    def test_a_missing_tool_is_a_clear_failure(self):
        with self.assertRaises(bench.Failure):
            bench.require_tool(Path("/nonexistent/k6"), "k6")

    def dataset(self, root, tool):
        lines = '{"resourceType":"Patient","id":"a"}\n'
        folder = root / "d"
        folder.mkdir(exist_ok=True)
        path = folder / "patient-observation-1.ndjson"
        path.write_text(lines, encoding="utf-8")
        sums = root / "SHA256SUMS"
        sums.write_text(f"{bench.sha256_file(path)}  {path.name}\n", encoding="utf-8")
        return bench.Dataset(1, folder, root / "prepared", tool, sums)

    def test_tampered_or_partial_derived_files_are_rebuilt(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            tool = root / "tool.py"
            tool.write_text("import pathlib,sys\np=pathlib.Path(sys.argv[2]);p.mkdir();(p/'batch-000001.json').write_text('{}')\n", encoding="utf-8")
            first = self.dataset(root, tool)
            batch = first.batches / "batch-000001.json"
            batch.write_text('{"tampered":true}', encoding="utf-8")
            with self.assertRaises(bench.Failure):
                first.check_before_load()
            second = self.dataset(root, tool)
            self.assertEqual(batch.read_text(encoding="utf-8"), "{}")
            second.check_before_load()


class HostSleepTests(unittest.TestCase):
    """A run in which the host slept is marked, not measured."""

    def test_a_sleep_is_the_wall_clock_running_ahead_of_the_monotonic_one(self):
        self.assertAlmostEqual(bench.seconds_asleep((1000.0, 50.0), (1296.0, 170.0)), 176.0)
        self.assertEqual(bench.seconds_asleep((1000.0, 50.0), (1119.9, 170.0)), 0.0)

    def test_status(self):
        self.assertEqual(bench.run_status({"s2xx": 10, "host_slept_s": 0.01}), "ok")
        self.assertEqual(bench.run_status({"s2xx": 10, "host_slept_s": 176.0}), "host_slept")
        self.assertEqual(bench.run_status({"s2xx": 0, "host_slept_s": 0.0}), "no_success")

    def test_the_summaries_show_it(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            base = dict(label="x", backend="postgres-sync-on", profile="3g", size=1, op="read", concurrency=8,
                        mode="open", target_rate=10, reset="snapshot", s2xx=5, throughput_rps=1.0)
            for number, (status, slept) in enumerate((("ok", 0.0), ("host_slept", 176.0)), start=1):
                run = root / f"run{number}"
                run.mkdir()
                (run / "run.json").write_text(json.dumps(dict(base, run=number, status=status, host_slept_s=slept)), encoding="utf-8")
            bench.summarize(root)
            runs = list(csv.DictReader((root / "runs.csv").open(encoding="utf-8")))
            self.assertEqual([(r["status"], r["host_slept_s"]) for r in runs], [("ok", "0.000"), ("host_slept", "176.000")])
            summary = list(csv.DictReader((root / "summary.csv").open(encoding="utf-8")))
            self.assertEqual((summary[0]["runs_ok"], summary[0]["host_slept_runs"]), ("1", "1"))
            part = {"sizes": [1], "ops": ["read"], "concurrency": [8], "runs": 2}
            counts = bench.part_counts([json.loads((root / f"run{n}" / "run.json").read_text()) for n in (1, 2)], part)
            self.assertEqual((counts["ok"], counts["host_slept"], counts["failed"], counts["left"]), (1, 1, 0, 1))


class StopSignalTests(unittest.TestCase):
    """run, calibrate and snapshot-check stop through their cleanup, as overnight does."""

    def setUp(self):
        self.saved = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

    def tearDown(self):
        for number, handler in self.saved.items():
            signal.signal(number, handler)

    def test_a_signal_runs_the_cleanup_and_exits_128_plus_its_number(self):
        cleaned = []

        def command(args):
            try:
                os.kill(os.getpid(), signal.SIGTERM)
                time.sleep(5)
            finally:
                cleaned.append(True)
            return 0

        self.assertEqual(bench.stops_cleanly(command)(argparse.Namespace()), 128 + signal.SIGTERM)
        self.assertEqual(cleaned, [True])

    def test_a_second_signal_does_not_interrupt_the_cleanup(self):
        cleaned = []

        def command(args):
            try:
                os.kill(os.getpid(), signal.SIGINT)
                time.sleep(5)
            finally:
                os.kill(os.getpid(), signal.SIGINT)
                cleaned.append(True)
            return 0

        self.assertEqual(bench.stops_cleanly(command)(argparse.Namespace()), 128 + signal.SIGINT)
        self.assertEqual(cleaned, [True])

    def test_a_first_signal_during_a_cleanup_waits_for_it(self):
        # The cleanup's own command runs to its end instead of being killed with the
        # harness's KeyboardInterrupt, and the stop takes effect after the block.
        finished = []

        def command(args):
            with bench.cleanup_shield():
                os.kill(os.getpid(), signal.SIGTERM)
                result = bench.run_cmd(["python3", "-c", "import time; time.sleep(1); print('done')"])
                finished.append(result.stdout.strip())
            finished.append("after the block")  # Never reached: the stop comes first.
            return 0

        self.assertEqual(bench.stops_cleanly(command)(argparse.Namespace()), 128 + signal.SIGTERM)
        self.assertEqual(finished, ["done"])
        self.assertEqual(bench.SHIELD, {"depth": 0, "pending": False})

    def test_a_shield_that_does_not_raise_leaves_the_stop_to_its_caller(self):
        stopping = bench.stop_on_signals()
        with bench.cleanup_shield(raise_after=False):
            os.kill(os.getpid(), signal.SIGINT)
            time.sleep(0.1)
        self.assertEqual(stopping, [signal.SIGINT])
        self.assertEqual(bench.SHIELD, {"depth": 0, "pending": False})

    def test_a_signal_outside_a_shield_still_stops_at_once(self):
        stopping = bench.stop_on_signals()
        with self.assertRaises(KeyboardInterrupt):
            os.kill(os.getpid(), signal.SIGINT)
            time.sleep(5)
        self.assertEqual(stopping, [signal.SIGINT])

    def test_nohup_still_keeps_sighup_ignored(self):
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        bench.stop_on_signals()
        self.assertEqual(signal.getsignal(signal.SIGHUP), signal.SIG_IGN)

    def test_the_commands_are_wrapped(self):
        for name in ("cmd_run", "cmd_calibrate", "cmd_snapshot_check"):
            self.assertIn("stops_cleanly", getattr(bench, name).__qualname__)

    def test_cleanup_commands_run_outside_the_terminals_process_group(self):
        script = ["python3", "-c", "import os; print(os.getpgrp())"]
        own = int(bench.run_cmd(script, own_session=True).stdout)
        shared = int(bench.run_cmd(script).stdout)
        self.assertEqual(shared, os.getpgrp())
        self.assertNotEqual(own, os.getpgrp())


class ReaperTests(unittest.TestCase):
    """A stop that the terminal cuts short is finished by a process of its own."""

    def setUp(self):
        self.saved = {key: (dict(value) if isinstance(value, dict) else value) for key, value in bench.CLEANUP.items()}
        bench.CLEANUP.update(plans={}, processes={}, overnight=None)
        self.dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        bench.CLEANUP.update(self.saved)
        bench.REAPER["plan"] = None
        if bench.LOG_SINK is not None:
            bench.LOG_SINK.close()
        bench.LOG_SINK = None

    def backend(self):
        args = argparse.Namespace(project="mine", db_port=None, profile="3g", baseline="on", reset="snapshot",
                                  keep_snapshots=False, built_snapshots=["mine_snapshot-postgres-10000-x"])
        return bench.Postgres(args), args

    def test_the_plan_names_the_database_the_snapshots_built_and_the_results(self):
        backend, args = self.backend()
        bench.CLEANUP["plans"][id(args)] = (backend, args, self.dir, [])
        bench.CLEANUP["processes"][12345] = "hfs"
        plan = bench.cleanup_plan("SIGHUP")
        (entry,) = plan["backends"]
        self.assertEqual(entry["compose"][:4], ["docker", "compose", "-p", "mine"])
        self.assertEqual(entry["volumes"], [backend.data_volume])
        self.assertEqual(entry["snapshots"], ["mine_snapshot-postgres-10000-x"])
        self.assertEqual(entry["out"], str(self.dir))
        self.assertEqual(plan["processes"], [{"pid": 12345, "name": "hfs"}])
        args.keep_snapshots = True  # overnight's snapshots outlive the stop
        self.assertEqual(bench.cleanup_plan("SIGHUP")["backends"][0]["snapshots"], [])

    def test_no_reaper_when_nothing_runs(self):
        with mock.patch.object(bench.subprocess, "Popen") as popen:
            bench.start_reaper("SIGINT")
        popen.assert_not_called()
        self.assertIsNone(bench.REAPER["plan"])

    def gone_pid(self) -> int:
        child = subprocess.Popen(["true"])
        child.wait()
        return child.pid

    def test_a_harness_that_finished_its_cleanup_leaves_nothing_to_do(self):
        plan = self.dir / "plan.json"
        plan.write_text(json.dumps({"reason": "SIGINT", "backends": [], "processes": [], "overnight": None}))
        plan.with_suffix(".done").write_text("")
        with mock.patch.object(bench, "run_cmd") as run_cmd:
            self.assertEqual(bench.cmd_reap(argparse.Namespace(pid=self.gone_pid(), plan=plan)), 0)
        run_cmd.assert_not_called()
        self.assertFalse(plan.exists())

    def test_a_killed_harness_is_finished_for_it(self):
        backend, args = self.backend()
        bench.CLEANUP["plans"][id(args)] = (backend, args, self.dir / "matrix", [])
        (self.dir / "matrix").mkdir()
        parts = [{"name": "postgres-on-10000", "backend": "postgres", "baseline": "on", "sizes": [10000],
                  "ops": ["create"], "concurrency": [8], "runs": 1, "out": "matrix"}]
        state_path = self.dir / "overnight-state.json"
        state_path.write_text(json.dumps({"parts": {}, "started_utc": "2026-11-10T02:00:00+00:00", "hours": 8,
                                          "budget_end_utc": "2026-11-10T10:00:00+00:00", "stopped_by": "running"}))
        bench.CLEANUP["overnight"] = {"out_root": str(self.dir), "state_path": str(state_path), "parts": parts,
                                      "resume_command": "python3 bench.py overnight --resume", "part": "postgres-on-10000"}
        plan = self.dir / "plan.json"
        plan.write_text(json.dumps(bench.cleanup_plan("SIGHUP")))
        with mock.patch.object(bench, "run_cmd", return_value=subprocess.CompletedProcess([], 0, "", "")) as run_cmd:
            self.assertEqual(bench.cmd_reap(argparse.Namespace(pid=self.gone_pid(), plan=plan)), 0)
        commands = [call.args[0] for call in run_cmd.call_args_list]
        self.assertIn(["down", "-v", "--remove-orphans"], [command[-3:] for command in commands])
        self.assertIn(["docker", "volume", "rm", "-f", backend.data_volume], commands)
        self.assertIn(["docker", "volume", "rm", "-f", "mine_snapshot-postgres-10000-x"], commands)
        down = next(call for call in run_cmd.call_args_list if call.args[0][-3:] == ["down", "-v", "--remove-orphans"])
        self.assertEqual(down.kwargs["env"][backend.port_var], str(backend.port))
        self.assertTrue((self.dir / "matrix" / "runs.csv").is_file())
        state = json.loads(state_path.read_text())
        self.assertEqual(state["parts"]["postgres-on-10000"]["status"], "interrupted by SIGHUP")
        self.assertIn("a separate process finished it", state["stopped_by"])
        summary = (self.dir / "morning-summary.md").read_text()
        self.assertIn("| postgres-on-10000 | interrupted by SIGHUP |", summary)
        self.assertIn("python3 bench.py overnight --resume", summary)
        self.assertIn("reaper: the cleanup is done", (self.dir / "overnight.log").read_text())
        self.assertFalse(plan.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
