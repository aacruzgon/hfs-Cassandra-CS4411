#!/usr/bin/env python3
"""Tests for fhir-versions-report.py, the per-FHIR-version table of the
`all-versions` job in .github/workflows/fhir-versions.yml. Standard library
only, as test_ci_report.py:

    python3 -I -m unittest discover -s .github/scripts -p 'test_fhir_versions_report.py'

The log excerpts are cut from real runs of the persistence and hfs test
binaries with HFS_TEST_FHIR_VERSION set (R5, and the invalid R7), the
`cargo test` lines around them as cargo prints them.
"""

from __future__ import annotations

import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("fhir_versions_report", HERE / "fhir-versions-report.py")
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)

LIB_R5 = """$ HFS_TEST_FHIR_VERSION=R5 cargo test -p helios-persistence --lib -- backends::cassandra --nocapture
     Running unittests src/lib.rs (target/debug/deps/helios_persistence-0123456789abcdef)

running 3 tests
fhir-versions: HFS_TEST_FHIR_VERSION=R5 -> R5 (5.0)
test backends::cassandra::errors::tests::refused_statements_are_query_errors ... ok
test backends::cassandra::write::tests::create_round_trips ... ok
test backends::cassandra::write::tests::needs_its_own_node ... ignored

test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 1839 filtered out; finished in 4.59s

exit=0
"""

# Two binaries in one cargo command: cassandra_tests prints the selector once;
# backend_capability_contract writes no resource and never asks for a version.
TESTS_R5 = """     Running tests/cassandra_tests.rs (target/debug/deps/cassandra_tests-44f4f5c84a5f2366)

running 2 tests
fhir-versions: HFS_TEST_FHIR_VERSION=R5 -> R5 (5.0)
test create_read::cassandra_create_then_read_round_trips ... ok
test create_read::cassandra_read_of_a_missing_id_is_none ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 9.18s

     Running tests/backend_capability_contract.rs (target/debug/deps/backend_capability_contract-3823f983977309cf)

running 2 tests
test sqlite::sqlite_declares_shared_schema_only ... ok
test cassandra::cassandra_instance_supports_matches_capabilities ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.21s

exit=0
"""

REST_R5 = """     Running tests/cassandra_milestone_a.rs (target/debug/deps/cassandra_milestone_a-26ca34be4cc4dec9)

running 2 tests
fhir-versions: HFS_TEST_FHIR_VERSION=R5 -> R5 (5.0), passed to hfs as HFS_DEFAULT_FHIR_VERSION=R5
test milestone_a_create_then_read_through_rest ... ok
[spec row=U-7:put-new expected=201 got=201]
test step3_crud_through_rest ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 4 filtered out; finished in 7.21s

exit=0
"""

# HFS_TEST_FHIR_VERSION=R7: the selector panics before printing, in every test.
TESTS_R7 = """
running 2 tests

thread 'create_read::cassandra_create_then_read_round_trips' (12782685) panicked at crates/persistence/src/backends/cassandra/mod.rs:91:17:
HFS_TEST_FHIR_VERSION="R7" is not a FHIR version this build enables
test create_read::cassandra_create_then_read_round_trips ... FAILED
test create_read::cassandra_read_of_a_missing_id_is_none ... FAILED

test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 161 filtered out; finished in 0.02s

exit=101
"""


class Scratch(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.logs = Path(self.tmp.name) / "logs"
        self.out = Path(self.tmp.name) / "out"
        self.logs.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, version, suite, text):
        (self.logs / f"fhir-{version}-{suite}.log").write_text(text)

    def run_report(self, *extra):
        with redirect_stdout(io.StringIO()) as printed:
            status = report.main([str(self.logs), str(self.out), *extra])
        self.assertEqual(status, 0)
        data = json.loads((self.out / "fhir-versions.json").read_text())
        self.assertEqual((self.out / "fhir-versions.md").read_text().strip(), printed.getvalue().strip())
        return data, {(r["version"], r["suite"]): r for r in data["rows"]}

    def write_r5(self):
        self.write("R5", "lib", LIB_R5)
        self.write("R5", "tests", TESTS_R5)
        self.write("R5", "rest", REST_R5)


class ParseLog(unittest.TestCase):
    def test_sums_the_binaries_of_one_command_and_keeps_each_selector(self):
        parsed = report.parse_log(TESTS_R5)
        self.assertEqual(parsed["exit"], 0)
        self.assertEqual([b["name"] for b in parsed["binaries"]],
                         ["tests/cassandra_tests.rs", "tests/backend_capability_contract.rs"])
        self.assertEqual([len(b["selectors"]) for b in parsed["binaries"]], [1, 0])
        self.assertEqual(parsed["binaries"][0]["selectors"][0],
                         {"value": "R5", "variant": "R5", "version": "5.0"})

    def test_strips_colour_and_reads_rc_as_the_exit_status(self):
        parsed = report.parse_log("\x1b[1m\x1b[92m     Running\x1b[0m tests/x.rs (target/x)\n"
                                  "running 1 test\ntest a ... ok\n\n"
                                  "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; "
                                  "0 filtered out; finished in 0.01s\n\nrc=0\n")
        self.assertEqual(parsed["binaries"][0]["name"], "tests/x.rs")
        self.assertEqual(parsed["exit"], 0)


class Report(Scratch):
    def test_all_three_suites_of_a_version_pass(self):
        self.write_r5()
        _, rows = self.run_report("--versions", "R5")
        lib, tests, rest = rows[("R5", "lib")], rows[("R5", "tests")], rows[("R5", "rest")]
        self.assertTrue(lib["ok"] and tests["ok"] and rest["ok"], [lib, tests, rest])
        self.assertEqual((lib["passed"], lib["ignored"]), (2, 1))
        self.assertEqual((tests["passed"], tests["binaries"]), (4, 2))
        self.assertEqual(tests["selector"], "ok (R5)")
        self.assertEqual(rest["selector"], "ok (R5)")

    def test_a_missing_selector_line_is_flagged_only_where_it_is_expected(self):
        self.write("R5", "lib", LIB_R5.replace("fhir-versions: HFS_TEST_FHIR_VERSION=R5 -> R5 (5.0)\n", ""))
        self.write("R5", "tests", TESTS_R5.replace("fhir-versions: HFS_TEST_FHIR_VERSION=R5 -> R5 (5.0)\n", "", 1))
        self.write("R5", "rest", REST_R5)
        data, rows = self.run_report("--versions", "R5")
        self.assertIn("no selector line: the version selector may not have applied", rows[("R5", "lib")]["problems"])
        self.assertIn("no selector line in 1 of 2 binaries (tests/cassandra_tests.rs)",
                      rows[("R5", "tests")]["problems"])
        self.assertTrue(rows[("R5", "rest")]["ok"])
        self.assertFalse(data["all_ok"])
        self.assertEqual(len(data["warnings"]), 2)

    def test_a_selector_naming_another_version_is_flagged(self):
        self.write_r5()
        self.write("R4", "lib", LIB_R5)
        _, rows = self.run_report("--versions", "R4")
        self.assertEqual(rows[("R4", "lib")]["selector"], "NOT OK")
        self.assertIn("selector names HFS_TEST_FHIR_VERSION=R5 -> R5, not R4 -> R4", rows[("R4", "lib")]["problems"])

    def test_a_version_number_must_select_its_variant(self):
        self.write("5.0", "lib", LIB_R5.replace("=R5 -> R5", "=5.0 -> R5"))
        _, rows = self.run_report("--versions", "5.0")
        self.assertEqual(rows[("5.0", "lib")]["selector"], "ok (R5)")
        self.write("5.0", "lib", LIB_R5.replace("=R5 -> R5", "=5.0 -> R4"))
        _, rows = self.run_report("--versions", "5.0")
        self.assertEqual(rows[("5.0", "lib")]["selector"], "NOT OK")

    def test_an_invalid_version_fails_with_its_exit_status_and_no_selector(self):
        self.write("R7", "tests", TESTS_R7)
        data, rows = self.run_report()
        self.assertEqual(data["versions"], ["R4", "R4B", "R5", "R6", "R7"])
        row = rows[("R7", "tests")]
        self.assertEqual((row["passed"], row["failed"], row["exit"]), (0, 2, 101))
        self.assertIn("exit status 101", row["problems"])
        self.assertIn("2 failed", row["problems"])
        self.assertIn("no selector line: the version selector may not have applied", row["problems"])
        self.assertEqual(rows[("R4", "lib")]["problems"], ["log missing"])

    def test_rest_must_run_exactly_its_two_tests_and_a_cut_off_run_is_flagged(self):
        self.write("R5", "rest", REST_R5.replace("2 passed", "1 passed").replace("exit=0\n", ""))
        _, rows = self.run_report("--versions", "R5")
        problems = rows[("R5", "rest")]["problems"]
        self.assertIn("1 tests ran, expected exactly 2", problems)
        self.assertIn("no `exit=` line: the run was cut off", problems)

    def test_a_filter_that_matches_nothing_is_flagged(self):
        self.write("R5", "lib", "running 0 tests\n\ntest result: ok. 0 passed; 0 failed; 0 ignored; "
                                "0 measured; 1951 filtered out; finished in 0.00s\n\nexit=0\n")
        _, rows = self.run_report("--versions", "R5")
        self.assertEqual(rows[("R5", "lib")]["problems"], ["no test passed: the filter matched nothing"])

    def test_bad_usage_exits_2(self):
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(report.main([str(self.logs)]), 2)
            self.assertEqual(report.main([str(self.logs), str(self.out), "--versions"]), 2)


if __name__ == "__main__":
    unittest.main()
