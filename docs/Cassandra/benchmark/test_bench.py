#!/usr/bin/env python3
"""Tests for bench.py's analysis. No Docker, k6 or server needed.

    python3 docs/Cassandra/benchmark/test_bench.py
"""

import csv
import gzip
import json
import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
