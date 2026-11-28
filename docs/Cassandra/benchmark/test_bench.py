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

    def test_a_second_signal_that_reenters_the_handler_starts_no_second_reaper(self):
        # A closed Rex pane sends two SIGHUPs about 1 ms apart. CPython runs a pending
        # handler at a call, so the second one can run inside the first, at the first call
        # it makes. Simulate that at the first call after the guard: signal.Signals.
        # (SIGTERM takes the same path; SIGHUP would also detach this test's output.)
        stopping = bench.stop_on_signals()
        handler = signal.getsignal(signal.SIGTERM)
        real_signals = signal.Signals
        nested = []

        def signals_reentering(number):
            if not nested:
                nested.append(True)
                handler(signal.SIGTERM, None)  # the second signal, run re-entrantly
            return real_signals(number)

        with mock.patch.object(bench, "start_reaper") as start_reaper, \
                mock.patch.object(bench.signal, "Signals", side_effect=signals_reentering):
            with bench.cleanup_shield(raise_after=False):
                handler(signal.SIGTERM, None)
        self.assertEqual(nested, [True])
        self.assertEqual(start_reaper.call_count, 1)
        self.assertEqual(stopping, [signal.SIGTERM])

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


class SamplerTests(unittest.TestCase):
    def test_the_sampler_keeps_the_first_and_last_cgroup_counters(self):
        sampler = bench.MemorySampler("c", {"hfs": 1, "k6": 2}, Path(tempfile.mkdtemp()) / "m.csv")
        answers = iter(["100\nanon 10\nfile 5\ninactive_file 2\n==cpu\nusage_usec 1000\nnr_throttled 0\n==io\n8:0 rbytes=10 wbytes=20 rios=1 wios=2\n",
                        "150\nanon 12\nfile 5\ninactive_file 2\n==cpu\nusage_usec 4000\nnr_throttled 3\n==io\n8:0 rbytes=10 wbytes=90 rios=1 wios=9\n"])
        with mock.patch.object(bench, "run_cmd", side_effect=lambda *a, **k: subprocess.CompletedProcess([], 0, next(answers), "")):
            first, second = sampler.sample_container(), sampler.sample_container()
        self.assertEqual((first["used_bytes"], second["current_bytes"]), (98, 150))
        self.assertEqual(sampler.cgroup_first["usage_usec"], 1000)
        self.assertEqual(sampler.cgroup_last["usage_usec"], 4000)
        self.assertEqual(sampler.cgroup_last["wbytes"], 90)

    def test_an_empty_or_garbled_answer_is_a_missed_sample(self):
        sampler = bench.MemorySampler("c", {"hfs": 1, "k6": 2}, Path(tempfile.mkdtemp()) / "memory.csv")
        for output in ("", "12\nanon\n"):
            with mock.patch.object(bench, "run_cmd", return_value=subprocess.CompletedProcess([], 0, output, "")):
                self.assertEqual(sampler.sample_container(), {})
        self.assertEqual(sampler.missed, 2)


    def test_missed_samples_reach_summary_csv_and_the_morning_summary(self):
        root = Path(tempfile.mkdtemp())
        for run, missed in ((1, 2), (2, 1)):
            (root / f"run{run}").mkdir()
            (root / f"run{run}" / "run.json").write_text(json.dumps(
                {"label": "laptop-dry-run", "backend": "cassandra", "size": 10000, "op": "read", "concurrency": 8,
                 "run": run, "status": "ok", "memory_samples_missed": missed}))
        bench.summarize(root)
        summary = list(csv.DictReader((root / "summary.csv").open()))
        self.assertEqual(summary[0]["memory_samples_missed"], "3")
        records = [json.loads((root / f"run{n}" / "run.json").read_text()) for n in (1, 2)]
        part = {"sizes": [10000], "ops": ["read"], "concurrency": [8], "runs": 2}
        self.assertEqual(bench.part_counts(records, part)["missed_samples"], 3)

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
        plan.with_suffix(".log").write_text("")
        with mock.patch.object(bench, "run_cmd") as run_cmd:
            self.assertEqual(bench.cmd_reap(argparse.Namespace(pid=self.gone_pid(), plan=plan)), 0)
        run_cmd.assert_not_called()
        self.assertFalse(plan.exists())
        self.assertFalse(plan.with_suffix(".log").exists())

    def test_a_second_reaper_waits_for_the_first_and_cleans_nothing(self):
        import fcntl
        import threading
        plan = self.dir / "plan.json"
        plan.write_text(json.dumps({"reason": "SIGHUP", "backends": [], "processes": [], "overnight": None}))
        holder = plan.open("rb")
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX)  # the first reaper, at work
        result = []
        with mock.patch.object(bench, "run_cmd") as run_cmd, mock.patch.object(bench, "summarize") as summarize:
            second = threading.Thread(target=lambda: result.append(
                bench.cmd_reap(argparse.Namespace(pid=self.gone_pid(), plan=plan))))
            second.start()
            time.sleep(0.5)
            self.assertTrue(second.is_alive())  # held at the lock, not cleaning alongside
            plan.unlink()  # the first reaper finishes: it removes its plan, then lets go
            holder.close()
            second.join(timeout=10)
        self.assertEqual(result, [0])
        run_cmd.assert_not_called()
        summarize.assert_not_called()

    def test_the_reaper_drops_an_earlier_nights_error_of_the_part_it_interrupts(self):
        parts = [{"name": "cassandra-10000", "backend": "cassandra", "sizes": [10000], "ops": ["read"],
                  "concurrency": [8], "runs": 1, "out": "matrix"}]
        (self.dir / "matrix").mkdir()
        state_path = self.dir / "overnight-state.json"
        state_path.write_text(json.dumps({"parts": {"cassandra-10000": {"seconds": 1.0, "status": "complete with failures",
                                                                        "error": "night 1: port 8180 is held"}},
                                          "started_utc": "2026-11-10T02:00:00+00:00", "hours": 8,
                                          "budget_end_utc": "2026-11-10T10:00:00+00:00", "stopped_by": "running"}))
        bench.finish_overnight({"out_root": str(self.dir), "state_path": str(state_path), "parts": parts,
                                "resume_command": "python3 bench.py overnight --resume", "part": "cassandra-10000"}, "SIGHUP")
        entry = json.loads(state_path.read_text())["parts"]["cassandra-10000"]
        self.assertEqual(entry["status"], "interrupted by SIGHUP")
        self.assertNotIn("error", entry)
        self.assertNotIn("night 1", (self.dir / "morning-summary.md").read_text())

    def test_the_reaper_adds_the_interrupted_parts_time(self):
        parts = [{"name": "postgres-on-10000", "backend": "postgres", "baseline": "on", "sizes": [10000],
                  "ops": ["read"], "concurrency": [8], "runs": 1, "out": "matrix"}]
        (self.dir / "matrix").mkdir()
        state_path = self.dir / "overnight-state.json"
        state_path.write_text(json.dumps({"parts": {"postgres-on-10000": {"seconds": 60.0}},
                                          "started_utc": "2026-11-10T02:00:00+00:00", "hours": 8,
                                          "budget_end_utc": "2026-11-10T10:00:00+00:00", "stopped_by": "running"}))
        overnight = {"out_root": str(self.dir), "state_path": str(state_path), "parts": parts,
                     "resume_command": "python3 bench.py overnight --resume", "part": "postgres-on-10000",
                     "part_started": 1000.0}
        bench.finish_overnight(overnight, "SIGHUP", ended=1285.0)
        entry = json.loads(state_path.read_text())["parts"]["postgres-on-10000"]
        self.assertAlmostEqual(entry["seconds"], 345.0)  # an earlier attempt's 60 s, and this one's 285 s
        self.assertIn("| postgres-on-10000 | interrupted by SIGHUP | 6 min |", (self.dir / "morning-summary.md").read_text())

    def test_a_command_refuses_to_start_while_a_reaper_is_at_work(self):
        import fcntl
        plan = self.dir / "hfsbench-cleanup-4242.json"
        plan.write_text("{}")
        (self.dir / "hfsbench-cleanup-17.json").write_text("{}")  # a leftover nobody holds
        with mock.patch.object(bench.tempfile, "gettempdir", return_value=str(self.dir)):
            self.assertIsNone(bench.reaper_at_work())
            holder = plan.open("rb")
            fcntl.flock(holder.fileno(), fcntl.LOCK_EX)  # a reaper at work
            with self.assertRaisesRegex(bench.Failure, "still being cleaned up .*hfsbench-cleanup-4242.log"):
                bench.require_no_reaper()
            holder.close()
            self.assertIsNone(bench.reaper_at_work())

    def test_a_copy_from_a_volume_that_is_gone_creates_nothing(self):
        backend, _ = self.backend()
        calls = []

        def fake(command, **kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command, 1 if command[:3] == ["docker", "volume", "inspect"] else 0, "", "")
        with mock.patch.object(bench, "run_cmd", side_effect=fake):
            with self.assertRaisesRegex(bench.Failure, "is gone"):
                backend._copy_volume("mine_snapshot-postgres-10000-x", backend.data_volume, "true")
        self.assertFalse([c for c in calls if c[:2] == ["docker", "run"]])

    def test_a_reaper_leaves_once_the_harness_has_finished_its_cleanup(self):
        # The harness writes .done and is still exiting: the reaper doesn't wait for it, and
        # a command started then isn't refused.
        import fcntl
        harness = subprocess.Popen(["sleep", "30"])
        try:
            plan = self.dir / f"hfsbench-cleanup-{harness.pid}.json"
            plan.write_text(json.dumps({"reason": "SIGINT", "backends": [], "processes": [], "overnight": None}))
            plan.with_suffix(".log").write_text("")
            holder = plan.open("rb")
            fcntl.flock(holder.fileno(), fcntl.LOCK_EX)  # its reaper, waiting
            plan.with_suffix(".done").write_text("")
            with mock.patch.object(bench.tempfile, "gettempdir", return_value=str(self.dir)):
                self.assertIsNone(bench.reaper_at_work())
            holder.close()
            started = time.monotonic()
            with mock.patch.object(bench, "run_cmd") as run_cmd:
                self.assertEqual(bench.cmd_reap(argparse.Namespace(pid=harness.pid, plan=plan)), 0)
            self.assertLess(time.monotonic() - started, 5)
            run_cmd.assert_not_called()
            self.assertFalse(plan.exists() or plan.with_suffix(".log").exists() or plan.with_suffix(".done").exists())
        finally:
            harness.kill()
            harness.wait()

    def test_the_reaper_leaves_a_part_the_harness_already_saved(self):
        # Killed after its own last save (stopped_by is no longer "running"): the part's time
        # and status are the harness's, and the reaper adds nothing to them.
        parts = [{"name": "postgres-on-10000", "backend": "postgres", "baseline": "on", "sizes": [10000],
                  "ops": ["read"], "concurrency": [8], "runs": 1, "out": "matrix"}]
        (self.dir / "matrix").mkdir()
        state_path = self.dir / "overnight-state.json"
        saved = {"seconds": 345.0, "status": "interrupted by SIGHUP"}
        state_path.write_text(json.dumps({"parts": {"postgres-on-10000": dict(saved)},
                                          "started_utc": "2026-11-10T02:00:00+00:00", "hours": 8,
                                          "budget_end_utc": "2026-11-10T10:00:00+00:00", "stopped_by": "SIGHUP"}))
        bench.finish_overnight({"out_root": str(self.dir), "state_path": str(state_path), "parts": parts,
                                "resume_command": "python3 bench.py overnight --resume", "part": "postgres-on-10000",
                                "part_started": 1000.0}, "SIGHUP", ended=1285.0)
        self.assertEqual(json.loads(state_path.read_text())["parts"]["postgres-on-10000"], saved)

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


def overnight_args(**overrides) -> argparse.Namespace:
    values = dict(ops="all", concurrency=[1, 8, 32], sizes=[10000, 100000], runs=3, duration=120, lwt_duration=600,
                  order="interleaved", purpose="checkpoint")
    values.update(overrides)
    return argparse.Namespace(**values)


class InterleavedOrderTests(unittest.TestCase):
    """pg-off-oddity: legs measured hours apart met a host 2-3x slower; the legs of a cell now run back to back."""

    def test_one_part_per_repetition_between_calibration_and_lwt(self):
        names = [part["name"] for part in bench.overnight_parts(overnight_args())]
        self.assertEqual(names, ["calibration", "matrix-run1", "matrix-run2", "matrix-run3",
                                 "lwt-steady-cassandra", "lwt-steady-postgres"])

    def test_blocks_keeps_the_old_order(self):
        names = [part["name"] for part in bench.overnight_parts(overnight_args(order="blocks"))]
        self.assertEqual(names, ["calibration", "postgres-on-10000", "postgres-on-100000", "cassandra-10000",
                                 "cassandra-100000", "postgres-off", "lwt-steady-cassandra", "lwt-steady-postgres"])

    def test_a_cells_legs_are_adjacent_and_off_runs_only_writes(self):
        part = bench.overnight_parts(overnight_args())[1]
        schedule = bench.interleaved_schedule(part)
        # 2 sizes x (2 write ops x 3 legs + 4 other ops x 2 legs) x 3 concurrencies
        self.assertEqual(len(schedule), 2 * (2 * 3 + 4 * 2) * 3)
        first = [bench.leg_label(leg) for size, op, vus, leg in schedule[:3]]
        self.assertEqual(first, ["postgres-sync-on", "cassandra", "postgres-sync-off"])
        self.assertEqual({schedule[i][:3] for i in range(3)}, {(10000, "create", 1)})
        self.assertNotIn("postgres-sync-off", [bench.leg_label(leg) for _, op, _, leg in schedule if op in ("read", "token")])
        # Every (cell, leg) of the matrix appears once in a repetition.
        self.assertEqual(len(set((size, op, vus, bench.leg_label(leg)) for size, op, vus, leg in schedule)), len(schedule))

    def test_each_leg_goes_first_once_in_three_repetitions(self):
        parts = bench.overnight_parts(overnight_args(sizes=[10000], ops="create", concurrency=[8]))[1:4]
        orders = [[bench.leg_label(leg) for *_, leg in bench.interleaved_schedule(part)] for part in parts]
        self.assertEqual(orders, [["postgres-sync-on", "cassandra", "postgres-sync-off"],
                                  ["cassandra", "postgres-sync-off", "postgres-sync-on"],
                                  ["postgres-sync-off", "postgres-sync-on", "cassandra"]])

    def test_an_interleaved_part_counts_its_own_repetition_and_declined_cells(self):
        part = bench.overnight_parts(overnight_args(sizes=[10000], ops="create,token", concurrency=[8]))[2]  # run 2
        records = [
            {"backend": "cassandra", "size": 10000, "op": "token", "concurrency": 8, "run": 1, "status": "not_implemented"},
            {"backend": "postgres-sync-on", "size": 10000, "op": "token", "concurrency": 8, "run": 2, "status": "ok"},
            {"backend": "postgres-sync-on", "size": 10000, "op": "create", "concurrency": 8, "run": 2, "status": "ok"},
            {"backend": "cassandra", "size": 10000, "op": "create", "concurrency": 8, "run": 2, "status": "failed"},
        ]
        counts = bench.part_counts(records, part)
        # on: create, token; cassandra: create, token; off: create = 5 cells, one run each.
        self.assertEqual(counts["total"], 5)
        self.assertEqual(counts["ok"], 2)
        self.assertEqual(counts["declined_cells"], 1)
        self.assertEqual(counts["failed"], 1)
        self.assertEqual(counts["left"], 2)

    def test_part_records_keeps_its_repetition_and_every_declined_cell(self):
        directory = Path(tempfile.mkdtemp())
        part = bench.overnight_parts(overnight_args(sizes=[10000], ops="create,token", concurrency=[8]))[2]
        for name, record in {
            "a": {"backend": "cassandra", "op": "token", "run": 1, "status": "not_implemented"},
            "b": {"backend": "cassandra", "op": "create", "run": 1, "status": "ok"},
            "c": {"backend": "cassandra", "op": "create", "run": 2, "status": "ok"},
            "d": {"backend": "postgres-sync-off", "op": "token", "run": 2, "status": "ok"},  # off has no token
        }.items():
            path = directory / "matrix" / name / "run.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(dict(record, label="checkpoint", size=10000, concurrency=8)))
        kept = sorted((r["op"], r["run"]) for r in bench.part_records(directory, part))
        self.assertEqual(kept, [("create", 2), ("token", 1)])


class DriftTests(unittest.TestCase):
    def record(self, backend, started, busy=False, cpu=10.0, run=1):
        return {"backend": backend, "size": 10000, "op": "create", "concurrency": 8, "run": run, "status": "ok",
                "measure_started_utc": started, "host_busy": busy, "host_foreign_cpu_max_pct": cpu}

    def test_the_span_between_legs_and_a_busy_host_on_one_leg_show(self):
        rows = bench.leg_comparisons([
            self.record("postgres-sync-on", "2026-11-28T01:00:00+00:00"),
            self.record("cassandra", "2026-11-28T01:04:30+00:00", busy=True, cpu=350.0),
            self.record("postgres-sync-off", "2026-11-28T01:09:00+00:00"),
            self.record("cassandra", "2026-11-28T05:00:00+00:00", run=2),  # alone in its repetition: no comparison
        ])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["span_min"], 9.0)
        self.assertTrue(rows[0]["host_busy_differs"])
        self.assertEqual(rows[0]["foreign_cpu_max_pct_spread"], 340.0)
        lines = "\n".join(bench.drift_lines(rows))
        self.assertIn("at most 9 min apart", lines)
        self.assertIn("1 of them met a busy host on one leg", lines)

    def test_blocks_hours_apart_show_as_a_long_span(self):
        rows = bench.leg_comparisons([self.record("postgres-sync-on", "2026-11-11T05:34:00+00:00"),
                                      self.record("postgres-sync-off", "2026-11-11T09:38:00+00:00")])
        self.assertEqual(rows[0]["span_min"], 244.0)
        self.assertFalse(rows[0]["host_busy_differs"])

    def test_the_morning_summary_has_the_section_and_the_markers(self):
        out = Path(tempfile.mkdtemp())
        state = {"parts": {"calibration": {"status": "complete", "seconds": 60}}, "started_utc": "2026-11-28T01:00:00+00:00",
                 "hours": 8, "budget_end_utc": "2026-11-28T09:00:00+00:00", "stopped_by": "finishing every part",
                 "banner": bench.PURPOSES["checkpoint"], "title": "Overnight checkpoint run"}
        parts = [{"name": "calibration", "kind": "calibrate", "backend": "postgres", "sizes": [10000], "ops": ["create"],
                  "concurrency": [8], "runs": 1, "out": "calibration", "label": "checkpoint-calibration"}]
        path = out / "calibration" / "postgres-sync-on" / "run.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"label": "checkpoint-calibration", "backend": "postgres-sync-on", "size": 10000,
                                    "op": "create", "concurrency": 8, "run": 1, "status": "ok"}))
        bench.write_morning_summary(out, state, parts, "python3 bench.py overnight --resume")
        summary = (out / "morning-summary.md").read_text()
        self.assertIn("# Overnight checkpoint run", summary)
        self.assertIn("CHECKPOINT RUN", summary)
        self.assertIn("## Host drift between legs", summary)
        self.assertTrue((out / "FINISHED").is_file())
        self.assertTrue((out / "COMPLETE").is_file())

    def test_no_complete_marker_while_something_is_left(self):
        out = Path(tempfile.mkdtemp())
        state = {"parts": {}, "started_utc": "2026-11-28T01:00:00+00:00", "hours": 8,
                 "budget_end_utc": "2026-11-28T09:00:00+00:00", "stopped_by": "SIGTERM"}
        parts = bench.overnight_parts(overnight_args(sizes=[10000], ops="create", concurrency=[8]))
        bench.write_morning_summary(out, state, parts, "resume")
        self.assertIn("SIGTERM", (out / "FINISHED").read_text())
        self.assertFalse((out / "COMPLETE").exists())


class ReaperScopeTests(unittest.TestCase):
    """pr76-delta4 m2: another project's stop no longer refuses this start; m1: the message says the log may go."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())

    def plan(self, name, project, out):
        path = self.dir / f"hfsbench-cleanup-{name}.json"
        path.write_text(json.dumps({"reason": "SIGINT", "processes": [], "overnight": None, "backends": [
            {"compose": ["docker", "compose", "-p", project, "-f", "x.yml"], "out": str(out)}]}))
        return path

    def test_only_a_stop_of_the_same_project_or_results_refuses(self):
        import fcntl
        theirs = self.plan("1", "otherbench-cassandra", self.dir / "their-results")
        holder = theirs.open("rb")
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX)
        try:
            with mock.patch.object(bench.tempfile, "gettempdir", return_value=str(self.dir)):
                self.assertIsNone(bench.reaper_at_work(["hfsbench-cassandra", "hfsbench-postgres"], [self.dir / "mine"]))
                # The same project, or the same results folder, still refuses.
                self.assertIsNotNone(bench.reaper_at_work(["otherbench-cassandra"], [self.dir / "mine"]))
                self.assertIsNotNone(bench.reaper_at_work(["hfsbench-cassandra"], [self.dir / "their-results" / "matrix"]))
                # Without a scope, as before: any held plan refuses.
                self.assertIsNotNone(bench.reaper_at_work())
                with self.assertRaisesRegex(bench.Failure, "is done or the log is gone"):
                    bench.require_no_reaper(["otherbench-cassandra"])
        finally:
            holder.close()

    def test_an_unreadable_plan_is_taken_as_ours(self):
        import fcntl
        path = self.dir / "hfsbench-cleanup-2.json"
        path.write_text("{not json")
        holder = path.open("rb")
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX)
        try:
            with mock.patch.object(bench.tempfile, "gettempdir", return_value=str(self.dir)):
                self.assertIsNotNone(bench.reaper_at_work(["hfsbench-cassandra"]))
        finally:
            holder.close()


class LinuxHostTests(unittest.TestCase):
    """On Linux `ps` gives lifetime CPU; /proc read twice gives the CPU of the last seconds."""

    def test_proc_stat_with_spaces_and_parentheses_in_the_name(self):
        line = "4242 (tmux: server (x)) S 1 4242 4242 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 1 0 100 0 0"
        self.assertEqual(bench.parse_proc_stat(line), (1, "tmux: server (x)", 300))
        self.assertIsNone(bench.parse_proc_stat("garbage"))

    def test_cpu_is_the_change_over_the_interval(self):
        before = {10: (1, "busy", 1000, 1000, "busy"), 11: (1, "idle", 5000, 1000, "idle")}
        after = {10: (1, "busy", 1400, 1000, "busy"), 11: (1, "idle", 5000, 1000, "idle"), 12: (1, "new", 90, 1000, "new")}
        cpu = bench.cpu_between(before, after, 2.0, 100)
        self.assertEqual(cpu[10][1], 200.0)  # 400 ticks in 2 s at 100 ticks/s: two cores
        self.assertEqual(cpu[11][1], 0.0)    # a lifetime total, however large, is not current load
        self.assertEqual(cpu[12][1], 0.0)    # born during the interval: unknown, not counted

    def test_the_system_dockers_plumbing_for_our_container_is_ours(self):
        processes = {
            1: (0, "dockerd", 0, 0, "/usr/bin/dockerd -H fd://"),
            2: (0, "docker-proxy", 0, 0, "/usr/bin/docker-proxy -proto tcp -host-ip 127.0.0.1 -host-port 59542 -container-port 9042"),
            3: (0, "docker-proxy", 0, 0, "/usr/bin/docker-proxy -proto tcp -host-ip 0.0.0.0 -host-port 8080 -container-port 80"),
            4: (0, "containerd-shim", 0, 0, "/usr/bin/containerd-shim-runc-v2 -namespace moby -id abcdef123456789 -address x"),
            5: (0, "containerd-shim", 0, 0, "/usr/bin/containerd-shim-runc-v2 -namespace moby -id 999999999999 -address x"),
            6: (1001, "dockerd", 0, 1001, "dockerd --rootless"),  # a CI runner's rootless Docker
        }
        self.assertEqual(sorted(bench.docker_plumbing(processes, 59542, "abcdef123456789")), [1, 2, 4])

    def test_a_ci_job_makes_the_host_busy_whatever_its_cpu(self):
        quiet = {"foreign_cpu_pct": 0.0, "ci_jobs": []}
        self.assertTrue(bench.host_is_quiet(quiet))
        self.assertFalse(bench.host_is_quiet(dict(quiet, ci_jobs=["77 /home/gh-runner/actions-runner/bin/Runner.Worker spawnclient"])))
        self.assertTrue(bench.is_ci_job("/home/gh-runner/actions-runner/bin/Runner.Worker spawnclient 1 2"))
        self.assertFalse(bench.is_ci_job("/home/gh-runner/actions-runner/bin/Runner.Listener run"))

    def test_the_watch_during_a_run_flags_a_job_that_started_after_the_reading_before(self):
        samples = [{"foreign_cpu_pct": 20.0, "ci_jobs": [], "processes_above_5pct": []},
                   {"foreign_cpu_pct": 410.0, "ci_jobs": ["77 Runner.Worker"], "processes_above_5pct": [{"pid": 9}]},
                   {"error": "docker did not answer"}]
        summary = bench.host_watch_summary(samples)
        self.assertTrue(summary["host_busy_during"])
        self.assertEqual(summary["host_busy_during_samples"], 1)
        self.assertEqual(summary["host_foreign_cpu_max_pct"], 410.0)
        self.assertEqual(summary["host_ci_jobs_during"], ["77 Runner.Worker"])
        self.assertEqual(summary["host_watch_failed_samples"], 1)
        quiet = bench.host_watch_summary([{"foreign_cpu_pct": 20.0, "ci_jobs": [], "processes_above_5pct": []}])
        self.assertFalse(quiet["host_busy_during"])
        self.assertTrue(bench.busy_overall({"host_busy_before": False, "host_busy_during": True}))
        self.assertFalse(bench.busy_overall({"host_busy_before": False, "host_busy_during": False}))

    def test_a_socket_on_the_port_in_any_state_is_found(self):
        table = ("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
                 "   0: 0100007F:E896 0100007F:1FF4 06 00000000:00000000 03:00000000 00000000     0        0 0\n"
                 "   1: 0100007F:1FF4 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 1\n")
        self.assertEqual(bench.sockets_on_port(0xE896, [table]), ["TIME_WAIT"])  # 59542
        self.assertEqual(bench.sockets_on_port(8180, [table]), ["LISTEN"])
        self.assertEqual(bench.sockets_on_port(55632, [table]), [])


class WritePathSettingsTests(unittest.TestCase):
    """Step 5's write path, named: the sweeper on, and timeouts hfs accepts (ADR-aacruzgon-0100)."""

    def cassandra(self):
        return bench.Cassandra(argparse.Namespace(project=None, db_port=None, profile="5g", baseline="on"))

    def test_cassandra_names_the_sweeper_and_a_request_timeout_above_the_connect_timeout(self):
        env = self.cassandra().hfs_env()
        self.assertEqual(env["HFS_CASSANDRA_SWEEPER_ENABLED"], "true")
        self.assertGreater(int(env["HFS_CASSANDRA_REQUEST_TIMEOUT_MS"]), int(env["HFS_CASSANDRA_CONNECT_TIMEOUT_MS"]))
        self.assertEqual(self.cassandra().hfs_log_checks(), (bench.CASSANDRA_SWEEPER_STARTED,))

    def test_an_exported_setting_never_reaches_hfs(self):
        env = bench.hfs_process_env(8180, self.cassandra().hfs_env(), {
            "PATH": "/usr/bin", "HFS_CASSANDRA_SWEEPER_ENABLED": "false", "HFS_CASSANDRA_POOL_SIZE": "1",
            "RUST_LOG": "trace", "OTEL_EXPORTER_OTLP_ENDPOINT": "http://x"})
        self.assertEqual(env["PATH"], "/usr/bin")
        self.assertEqual(env["HFS_CASSANDRA_SWEEPER_ENABLED"], "true")
        self.assertNotIn("HFS_CASSANDRA_POOL_SIZE", env)
        self.assertNotIn("OTEL_EXPORTER_OTLP_ENDPOINT", env)
        self.assertEqual(env["RUST_LOG"], "warn,hfs=info")

    def test_the_line_hfs_main_logs_is_the_one_checked(self):
        main = (bench.REPO / "crates" / "hfs" / "src" / "main.rs").read_text(encoding="utf-8")
        self.assertIn(f'info!("{bench.CASSANDRA_SWEEPER_STARTED}")', main)

    def test_a_snapshot_is_named_for_its_schema_and_commit(self):
        backend = self.cassandra()
        dataset = argparse.Namespace(size=10000, sha256="a" * 64)
        name = backend.snapshot_volume(dataset, "1a2b3c4d5")
        self.assertIn("-1a2b3c4d5-", name)
        self.assertTrue(name.endswith(backend.schema_sha256()[:8]))
        with mock.patch.object(bench.Cassandra, "schema_sha256", return_value="f" * 64):
            self.assertNotEqual(backend.snapshot_volume(dataset, "1a2b3c4d5"), name)

    def test_max_vus_is_the_concurrency_unless_a_factor_is_chosen(self):
        self.assertEqual(bench.max_vus(argparse.Namespace(), 8), 8)
        self.assertEqual(bench.max_vus(argparse.Namespace(max_vus_factor=4), 8), 32)
        script = (bench.HERE / "load.js").read_text(encoding="utf-8")
        self.assertIn("maxVUs: MAX_VUS", script)



ORIGINAL_RUN_FIELDS = [
    "label", "backend", "profile", "size", "op", "concurrency", "mode", "target_rate", "run",
    "status", "latency_reading", "duration_s", "completed", "offered", "drop_pct", "throughput_rps", "ok_rps",
    "p50_ms", "p95_ms", "p99_ms", "p50_ok_ms", "p95_ok_ms", "p99_ok_ms",
    "max_ms", "s2xx", "s409", "s412", "s4xx_other", "s501", "s5xx_other", "s_other", "statuses",
    "dropped_iterations", "saturated", "steady_change_pct", "reset", "mem_peak_used_mib", "mem_peak_anon_mib",
    "memory_peak_file_mib", "mem_limit_mib", "hfs_peak_rss_mib", "k6_peak_rss_mib", "oom_killed",
    "synchronous_commit", "hfs_commit", "hfs_build_profile", "dataset_file", "dataset_sha256",
    "dataset_verified", "memory_samples_missed", "host_busy", "host_load_1m", "host_foreign_cpu_pct",
    "host_docker_limits_gib", "host_slept_s", "measure_wall_s", "failure",
]
ORIGINAL_SUMMARY_FIELDS = [
    "label", "backend", "profile", "size", "op", "concurrency", "mode", "target_rate", "synchronous_commit",
    "dataset_file", "dataset_sha256", "dataset_verified",
    "cell_status", "latency_reading", "saturated_runs", "runs_ok", "runs_recorded", "throughput_rps_mean", "throughput_rps_min",
    "throughput_rps_max", "throughput_cv_pct", "ok_rps_mean", "drop_pct_mean", "p50_ms_mean", "p50_cv_pct",
    "p95_ms_mean", "p95_cv_pct", "p99_ms_mean", "p50_ok_ms_mean", "p95_ok_ms_mean", "p99_ok_ms_mean", "s2xx", "s409", "s412", "s4xx_other", "s501",
    "s5xx_other", "s_other", "dropped_iterations", "steady_change_pct_mean", "reset", "mem_peak_used_mib",
    "mem_peak_anon_mib", "memory_peak_file_mib", "mem_limit_mib", "hfs_peak_rss_mib",
    "k6_peak_rss_mib", "oom_killed", "host_busy_runs", "host_slept_runs", "memory_samples_missed",
]


class MetricsTests(unittest.TestCase):
    """Follow-up 1: as many metrics as the harness can cheaply collect, all report-only."""

    def analysed(self, rows):
        directory = Path(tempfile.mkdtemp())
        source = directory / "k6.csv.gz"
        write_k6_csv(source, rows)
        metrics = bench.analyse(source, directory / "requests.csv.gz", directory / "bins.csv", 10, 10,
                                directory / "histogram.csv")
        return metrics, directory

    def test_every_percentile_min_mean_and_stddev(self):
        rows = [("http_req_duration", 1000 + n, float(n), "201") for n in range(1, 1001)]
        metrics, _ = self.analysed(rows)
        self.assertEqual(metrics["p90_ms"], 900.0)
        self.assertEqual(metrics["p999_ms"], 999.0)
        self.assertEqual(metrics["min_ms"], 1.0)
        self.assertEqual(metrics["max_ok_ms"], 1000.0)
        self.assertAlmostEqual(metrics["mean_ms"], 500.5)
        self.assertAlmostEqual(metrics["stddev_ms"], 288.6749, places=3)
        self.assertEqual(metrics["n"], 1000)
        self.assertEqual(metrics["n_ok"], 1000)

    def test_a_tail_percentile_is_flagged_without_ten_requests_beyond_it(self):
        self.assertFalse(bench.reliable(9999, 0.999))
        self.assertTrue(bench.reliable(10000, 0.999))
        self.assertFalse(bench.reliable(99999, 0.9999))
        self.assertTrue(bench.reliable(100000, 0.9999))
        metrics, _ = self.analysed([("http_req_duration", 1000 + n, 1.0, "200") for n in range(50)])
        self.assertFalse(metrics["p999_reliable"])
        self.assertEqual(metrics["p999_ms"], 1.0)  # kept, only flagged

    def test_the_intervals_hold_their_percentile_and_are_reproducible(self):
        import random
        rng = random.Random(7)
        values = sorted(rng.expovariate(1.0) for _ in range(5000))
        first, second = bench.intervals(values), bench.intervals(values)
        self.assertEqual(first, second)  # a fixed seed
        self.assertIn("bootstrap 200 resamples", first["ci_method"])
        for name, fraction in (("p95", 0.95), ("p99", 0.99)):
            self.assertLessEqual(first[f"{name}_ci_low_ms"], bench.percentile(values, fraction))
            self.assertGreaterEqual(first[f"{name}_ci_high_ms"], bench.percentile(values, fraction))
        many = sorted(rng.expovariate(1.0) for _ in range(bench.BOOTSTRAP_MAX_N + 1))
        large = bench.intervals(many)
        self.assertIn("order statistic", large["ci_method"])
        self.assertLess(large["p95_ci_low_ms"], large["p95_ci_high_ms"])

    def test_the_histogram_has_log_buckets_counted_per_status_class(self):
        rows = [("http_req_duration", 1000, 0.05, "201"), ("http_req_duration", 1001, 1.3, "201"),
                ("http_req_duration", 1002, 1.5, "409"), ("http_req_duration", 1003, 90000.0, "504")]
        _, directory = self.analysed(rows)
        with (directory / "histogram.csv").open() as source:
            histogram = list(csv.DictReader(source))
        self.assertEqual(histogram[0]["lower_ms"], "0")
        self.assertEqual(histogram[0]["s2xx"], "1")       # below 0.1 ms
        bucket = next(row for row in histogram if row["lower_ms"] == "1.259")  # 10^0.1 to 10^0.2: 1.259-1.585 ms
        self.assertEqual((bucket["s2xx"], bucket["s409"]), ("1", "1"))
        self.assertEqual(histogram[-1]["upper_ms"], "")    # the open top bucket
        self.assertEqual(histogram[-1]["s5xx_other"], "1")
        self.assertEqual(sum(int(row["total"]) for row in histogram), 4)
        self.assertEqual(len(histogram), 2 + bench.HISTOGRAM_HIGH_EXP - bench.HISTOGRAM_LOW_EXP)

    def test_latency_by_status_class_and_k6s_phases(self):
        rows = [("http_req_duration", 1000, 2.0, "201"), ("http_req_duration", 1001, 40.0, "409"),
                ("http_req_waiting", 1000, 1.5, "201"), ("http_req_waiting", 1001, 39.0, "409"),
                ("http_req_blocked", 1000, 0.01, "201"), ("data_sent", 1000, 300.0, ""), ("data_received", 1000, 900.0, "")]
        metrics, _ = self.analysed(rows)
        self.assertEqual(metrics["s409_p50_ms"], 40.0)
        self.assertEqual(metrics["s2xx_max_ms"], 2.0)
        self.assertIsNone(metrics["s5xx_other_p99_ms"])
        self.assertEqual(metrics["k6_waiting_p99_ms"], 39.0)
        self.assertEqual(metrics["k6_blocked_p50_ms"], 0.01)
        self.assertIsNone(metrics["k6_connecting_p50_ms"])
        self.assertEqual((metrics["k6_data_sent_bytes"], metrics["k6_data_received_bytes"]), (300, 900))
        self.assertEqual(metrics["completed"], 2)  # phase rows are not requests

    def test_the_csvs_keep_every_earlier_column_in_its_place(self):
        self.assertEqual(bench.RUN_FIELDS[:len(ORIGINAL_RUN_FIELDS)], ORIGINAL_RUN_FIELDS)
        self.assertEqual(bench.SUMMARY_FIELDS[:len(ORIGINAL_SUMMARY_FIELDS)], ORIGINAL_SUMMARY_FIELDS)
        self.assertEqual(len(set(bench.RUN_FIELDS)), len(bench.RUN_FIELDS))
        self.assertEqual(len(set(bench.SUMMARY_FIELDS)), len(bench.SUMMARY_FIELDS))
        for name in ("p99_ms_cv_pct", "max_ok_ms_min", "p9999_ok_ms_max", "p90_ms_mean"):
            self.assertIn(name, bench.SUMMARY_FIELDS)

    def test_the_summary_spreads_every_percentile_over_the_runs(self):
        root = Path(tempfile.mkdtemp())
        for number, p99 in ((1, 10.0), (2, 20.0)):
            path = root / "cassandra" / f"run{number}" / "run.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"backend": "cassandra", "size": 1, "op": "read", "concurrency": 1, "run": number,
                                        "status": "ok", "p99_ms": p99, "max_ok_ms": p99 * 2, "max_vus": 4}))
        bench.summarize(root)
        with (root / "summary.csv").open() as source:
            row = next(csv.DictReader(source))
        self.assertEqual((row["p99_ms_min"], row["p99_ms_max"], row["p99_ms_mean"]), ("10.000", "20.000", "15.000"))
        self.assertEqual(row["max_ok_ms_max"], "40.000")
        self.assertEqual(row["max_vus"], "4")
        self.assertAlmostEqual(float(row["p99_ms_cv_pct"]), 47.140, places=2)


TPSTATS = """Pool Name                         Active Pending Completed Blocked All time blocked
MutationStage                          0       2     184213       0                0
ReadStage                              1       0      90211       0                3

Latencies waiting in queue (micros) per dropped message types
Message type                      Dropped                  Latency waiting in queue (micros)
                                                     50%               95%               99%               Max
MUTATION_REQ                           7               0.0               0.0               0.0               0.0
READ_REQ                               0               0.0               0.0               0.0               0.0
"""


class WindowCounterTests(unittest.TestCase):
    def test_tpstats_pools_and_dropped_messages(self):
        values = bench.parse_tpstats(TPSTATS)
        self.assertEqual(values["pool.MutationStage.completed"], 184213)
        self.assertEqual(values["pool.MutationStage.pending"], 2)
        self.assertEqual(values["pool.ReadStage.all_time_blocked"], 3)
        self.assertEqual(values["dropped.MUTATION_REQ"], 7)
        self.assertEqual(values["dropped.READ_REQ"], 0)

    def test_cgroup_cpu_and_io(self):
        cpu = bench.parse_cpu_stat("usage_usec 5000000\nuser_usec 4000000\nnr_throttled 3\nthrottled_usec 250000\n")
        self.assertEqual((cpu["usage_usec"], cpu["nr_throttled"]), (5000000, 3))
        io = bench.parse_io_stat("259:0 rbytes=1048576 wbytes=2097152 rios=10 wios=20 dbytes=0 dios=0\n"
                                 "8:0 rbytes=1048576 wbytes=0 rios=1 wios=0\n")
        self.assertEqual(io, {"rbytes": 2097152, "wbytes": 2097152, "rios": 11, "wios": 20})

    def test_the_window_is_after_less_before_and_its_columns(self):
        before = {"cgroup.usage_usec": 1_000_000, "cgroup.wbytes": 0, "db.dropped.MUTATION_REQ": 2,
                  "db.pool.MutationStage.pending": 0, "db.pool.ReadStage.all_time_blocked": 1,
                  "db.wal.wal_bytes": 1024**2, "hfs.cpu_s": 10.0}
        after = {"cgroup.usage_usec": 4_500_000, "cgroup.wbytes": 3 * 1024**2, "db.dropped.MUTATION_REQ": 9,
                 "db.pool.MutationStage.pending": 5, "db.pool.ReadStage.all_time_blocked": 1,
                 "db.wal.wal_bytes": 5 * 1024**2, "hfs.cpu_s": 12.5, "host.cpu_mhz_mean": 3800.0}
        window = {"before": before, "after": after, "delta": bench.window_delta(before, after)}
        row = bench.window_columns(window)
        self.assertEqual(row["db_cpu_s"], 3.5)
        self.assertEqual(row["db_io_write_mib"], 3.0)
        self.assertEqual(row["hfs_cpu_s"], 2.5)
        self.assertEqual(row["pg_wal_mib"], 4.0)
        self.assertEqual(row["host_cpu_mhz_mean"], 3800.0)
        self.assertEqual(row["cassandra_dropped_messages"], 7)
        self.assertEqual(row["cassandra_pending_after"], 5)
        self.assertEqual(row["cassandra_blocked"], 0)
        self.assertEqual(bench.window_columns({}), {})

    def test_postgres_json_is_flattened(self):
        values = {}
        bench.flatten("db", {"db": {"xact_commit": 5, "blks_hit": "12"}, "lock_waits": 0, "wal": None}, values)
        self.assertEqual(values, {"db.db.xact_commit": 5, "db.db.blks_hit": 12.0, "db.lock_waits": 0})

    def test_a_reading_that_fails_is_left_out_not_fatal(self):
        class Broken:
            def window_counters(self):
                return {"dropped": {"READ_REQ": 1}}

        hfs = argparse.Namespace(process=argparse.Namespace(pid=os.getpid()))
        class Failing(Broken):
            def window_counters(self):
                raise bench.Failure("nodetool did not answer")

        values = bench.read_window(Broken(), hfs, "c")
        self.assertNotIn("cgroup.usage_usec", values)  # the sampler reads the cgroup, not the window's edge
        self.assertEqual(values["db.dropped.READ_REQ"], 1)
        self.assertEqual(bench.read_window(Failing(), hfs, "c")["db.error"], 1)
        self.assertIsNotNone(values.get("hfs.cpu_s"))

    def test_the_own_process_cpu_time_reads(self):
        seconds = bench.process_cpu_seconds(os.getpid())
        self.assertIsNotNone(seconds)
        self.assertGreater(seconds, 0.0)

    def test_the_extra_collection_can_be_turned_off(self):
        self.assertTrue(bench.extra_metrics(argparse.Namespace()))
        self.assertFalse(bench.extra_metrics(argparse.Namespace(extra_metrics="off")))

    def test_cassandra_reads_its_counters_before_the_warm_up_and_postgres_does_not(self):
        args = argparse.Namespace(project=None, db_port=None, profile="5g", baseline="on")
        cassandra, postgres = bench.Cassandra(args), bench.Postgres(args)
        with mock.patch.object(bench.Cassandra, "window_counters", return_value={"dropped.READ_REQ": 1}):
            self.assertEqual(bench.early_db_counters(argparse.Namespace(), cassandra), {"dropped.READ_REQ": 1})
            self.assertIsNone(bench.early_db_counters(argparse.Namespace(extra_metrics="off"), cassandra))
            self.assertIsNone(bench.early_db_counters(argparse.Namespace(skip_collectors="window"), cassandra))
        self.assertIsNone(bench.early_db_counters(argparse.Namespace(), postgres))
        # The window then takes that reading as its "before".
        hfs = argparse.Namespace(process=argparse.Namespace(pid=os.getpid()))
        with mock.patch.object(bench.Cassandra, "window_counters", side_effect=AssertionError("read again")):
            values = bench.read_window(cassandra, hfs, "c", {"dropped": {"READ_REQ": 4}})
        self.assertEqual(values["db.dropped.READ_REQ"], 4)

    def test_collectors_can_be_skipped_one_at_a_time_and_a_typo_is_refused(self):
        self.assertEqual(bench.skipped_collectors(argparse.Namespace()), set())
        self.assertEqual(bench.skipped_collectors(argparse.Namespace(skip_collectors="window, host-watch")),
                         {"window", "host-watch"})
        with self.assertRaisesRegex(bench.Failure, "unknown collector"):
            bench.skipped_collectors(argparse.Namespace(skip_collectors="metrics"))


class MorningLatencyTests(unittest.TestCase):
    def test_the_table_shows_p99_and_max(self):
        directory = Path(tempfile.mkdtemp())
        with (directory / "summary.csv").open("w", newline="") as sink:
            writer = csv.DictWriter(sink, fieldnames=bench.SUMMARY_FIELDS)
            writer.writeheader()
            writer.writerow({"backend": "cassandra", "size": "10000", "op": "read", "concurrency": "8", "max_vus": "8",
                             "runs_ok": "3", "ok_rps_mean": "100.0", "drop_pct_mean": "0.5", "p50_ok_ms_mean": "1.0",
                             "p95_ok_ms_mean": "2.0", "p99_ok_ms_mean": "3.25", "max_ok_ms_mean": "40.5", "saturated_runs": "0"})
            writer.writerow({"backend": "cassandra", "size": "10000", "op": "token", "concurrency": "8", "runs_ok": "0"})
        lines = bench.latency_lines(directory)
        self.assertIn("| p99 | max |", lines[2])
        self.assertEqual(len(lines), 5)  # the declined cell has no row
        self.assertIn("| 3.25 | 40.50 |", lines[4])



class LightWatchTests(unittest.TestCase):
    """The host watch reads /proc/stat and the harness's own ticks, not every process (overhead control)."""

    def test_non_idle_ticks_leave_idle_and_iowait_out(self):
        text = "cpu  100 5 50 9000 70 3 2 1 0 0\ncpu0 50 2 25 4500 35 1 1 0 0 0\n"
        self.assertEqual(bench.host_busy_ticks(text), 100 + 5 + 50 + 3 + 2 + 1)
        self.assertIsNone(bench.host_busy_ticks("intr 1 2 3\n"))

    def test_own_processes_are_the_roots_and_their_descendants(self):
        processes = {1: (0, "init", 0, 0, ""), 10: (1, "python3", 0, 0, ""), 11: (10, "hfs", 0, 0, ""),
                     12: (11, "worker", 0, 0, ""), 20: (1, "containerd-shim", 0, 0, ""), 21: (20, "java", 0, 0, ""),
                     30: (1, "cargo", 0, 0, "")}
        self.assertEqual(bench.own_processes(processes, [10, 20]), [10, 11, 12, 20, 21])

    def test_the_first_reading_is_a_baseline_and_is_not_a_sample(self):
        watch = bench.HostWatch(argparse.Namespace(port=1), every_seconds=0.01)
        watch.linux = True
        readings = iter([({"foreign_cpu_pct": 0.0, "ci_jobs": [], "processes_above_5pct": [], "baseline": True}, (0, 0, 0)),
                         ({"foreign_cpu_pct": 350.0, "ci_jobs": [], "processes_above_5pct": []}, (1, 1, 1))])

        def reading(last):
            try:
                return next(readings)
            except StopIteration:
                watch.halt.set()
                return {"foreign_cpu_pct": 0.0, "ci_jobs": [], "processes_above_5pct": [], "baseline": True}, (2, 2, 2)

        with mock.patch.object(bench, "HOST_WATCH_FIRST_SECONDS", 0), mock.patch.object(watch, "linux_reading", side_effect=reading):
            watch.run()
        self.assertEqual([s["foreign_cpu_pct"] for s in watch.samples], [350.0])
        self.assertTrue(watch.summary()["host_busy_during"])

    @unittest.skipUnless(Path("/proc/self/stat").exists(), "Linux only")
    def test_this_process_ticks_are_read(self):
        self.assertIsNotNone(bench.proc_ticks(os.getpid()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
