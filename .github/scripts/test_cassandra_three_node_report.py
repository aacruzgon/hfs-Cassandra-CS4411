#!/usr/bin/env python3
"""Tests for .github/scripts/cassandra-three-node/report.py's failure classes
(the three-node job, ADR-aacruzgon-0083). Standard library only:

    PYTHONPATH=.github/scripts python3 -m unittest -v test_cassandra_three_node_report

The logs are libtest output in the shape the job's test steps write
(run-tests.sh: a cargo-style "Running" line, then the binary's output). The
cases A to G are the independent audit's synthetic logs for PR #101
(2026-11-20); the panic messages follow the race suites'
(`versioned_write_race_suite.rs`), PR #83's invariant panics and
`node_runs_the_docker_cassandra_configuration`'s assertions.
"""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("three_node_report", HERE / "cassandra-three-node" / "report.py")
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)

SERIAL_READ_504 = (
    'Timeout { backend_name: "cassandra", message: "serial read: CAS operation timed out: '
    'received 0 of 2 required responses after 0 contention retries" }'
)
RACE = "crates/persistence/tests/search/versioned_write_race_suite.rs:96:22"


def captured(test: str, location: str, message: str) -> str:
    """A run in libtest's default capture mode where `test` fails with
    `message` (one or more lines) at `location`."""
    return f"""     Running tests/cassandra_tests.rs (/x/cassandra_tests-abc)

running 2 tests
test ok_test ... ok
test {test} ... FAILED

failures:

---- {test} stdout ----

thread '{test}' (162) panicked at {location}:
{message}
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    {test}

test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s
"""


def loser(error: str) -> str:
    return f"round 0: a loser is a concurrency refusal, got Failed(Backend({error}))"


class Classes(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name)
        (self.out / "logs").mkdir()
        (self.out / "run").mkdir()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def log(self, label: str, text: str, run: dict | None = None) -> None:
        (self.out / "logs" / f"{label}.log").write_text(text)
        if run is not None:
            (self.out / "run" / f"{label}.json").write_text(json.dumps({"label": label, **run}))

    def failures(self, label: str, text: str, run: dict | None = None) -> list[dict]:
        self.log(label, text, run)
        run_file = self.out / "run" / f"{label}.json"
        run = json.loads(run_file.read_text()) if run_file.exists() else None
        stopped = "timeout" if run and run.get("timed_out") else "killed" if run and run.get("killed") else None
        return report.parse_log(self.out / "logs" / f"{label}.log", stopped=stopped)["failures"]

    def only(self, label: str, text: str, run: dict | None = None) -> dict:
        found = self.failures(label, text, run)
        self.assertEqual(len(found), 1, found)
        return found[0]

    # A: the race suites' #77 504, the serial read's timeout.
    def test_a_serial_read_timeout_is_issue_77(self) -> None:
        f = self.only("A-77-r1", captured("step3_suites::race_a", RACE, loser(SERIAL_READ_504)))
        self.assertEqual(f["class"], "504-77")
        self.assertEqual(f["location"], RACE)
        self.assertTrue(f["first_line"].startswith("round 0: a loser is a concurrency refusal"))

    # B: a plain write's timeout is a 504, not #77.
    def test_b_write_timeout_is_another_504(self) -> None:
        error = 'Timeout { backend_name: "cassandra", message: "update: Operation timed out - received only 1 responses." }'
        f = self.only("B-504-plain-r1", captured("step3_suites::race_b", RACE, loser(error)))
        self.assertEqual(f["class"], "504-other")

    # B2 (audit S2): a CAS write's timeout has the CAS text, without the
    # serial read: another 504.
    def test_b2_cas_write_timeout_is_another_504(self) -> None:
        error = ('Timeout { backend_name: "cassandra", message: "create: CAS operation timed out: '
                 'received 1 of 2 required responses after 3 contention retries" }')
        f = self.only("B2-504-caswrite-r1", captured("step3_suites::race_b2", RACE, loser(error)))
        self.assertEqual(f["class"], "504-other")

    # B3: the unknown-outcome 504.
    def test_b3_unknown_outcome_is_another_504(self) -> None:
        error = 'Timeout { backend_name: "cassandra", message: "update: the compare-and-swap\'s outcome is unknown" }'
        f = self.only("B3-504-unknown-r1", captured("step3_suites::race_b3", RACE, loser(error)))
        self.assertEqual(f["class"], "504-other")

    # C: an unwrap of a 503.
    def test_c_unavailable_is_503(self) -> None:
        message = ('called `Result::unwrap()` on an `Err` value: Backend(Unavailable { backend_name: "cassandra", '
                   'message: "Cassandra cannot serve the request right now" })')
        f = self.only("C-503-r1", captured("step3_suites::c503", "crates/persistence/tests/cassandra_tests.rs:120:10", message))
        self.assertEqual(f["class"], "503")

    # The --lib tests unwrap a BackendError itself, with no StorageError around it.
    def test_lib_unwrap_of_a_bare_timeout_is_issue_77(self) -> None:
        message = f"called `Result::unwrap()` on an `Err` value: {SERIAL_READ_504}"
        f = self.only("lib-r1", captured("backends::cassandra::write::node_tests::x", "crates/persistence/src/x.rs:1:1", message))
        self.assertEqual(f["class"], "504-77")

    # D and D2: the two failures the README expects on three nodes.
    def test_d_rack_assertion_is_expected(self) -> None:
        message = 'assertion `left == right` failed\n  left: "helios-rack2"\n right: "helios-rack1"'
        f = self.only("D-rack-r1", captured("node_runs_the_docker_cassandra_configuration",
                                            "crates/persistence/tests/cassandra_tests.rs:185:5", message))
        self.assertEqual(f["class"], "expected")

    def test_d2_one_node_no_peers_is_expected(self) -> None:
        message = ("assertion `left == right` failed: one node, no peers: "
                   "[10.59.142.11:9042, 10.59.142.12:9042, 10.59.142.13:9042]\n  left: 3\n right: 1")
        f = self.only("D2-peers-r1", captured("node_runs_the_docker_cassandra_configuration",
                                              "crates/persistence/tests/cassandra_tests.rs:214:5", message))
        self.assertEqual(f["class"], "expected")

    # D3 (audit S6): the same test failing on another assertion is not expected.
    def test_d3_release_mismatch_is_other(self) -> None:
        message = 'assertion `left == right` failed\n  left: "5.0.8"\n right: "5.0.9"'
        f = self.only("D3-release-r1", captured("node_runs_the_docker_cassandra_configuration",
                                                "crates/persistence/tests/cassandra_tests.rs:183:5", message))
        self.assertEqual(f["class"], "other")
        self.assertIn("README", f["note"])

    def test_d4_data_centre_mismatch_is_other(self) -> None:
        message = 'assertion `left == right` failed\n  left: "dc2"\n right: "helios-dc1"'
        f = self.only("D4-dc-r1", captured("node_runs_the_docker_cassandra_configuration",
                                           "crates/persistence/tests/cassandra_tests.rs:184:5", message))
        self.assertEqual(f["class"], "other")

    # E: an assertion with no backend error.
    def test_e_plain_assertion_is_other(self) -> None:
        message = 'assertion `left == right` failed: version history has a gap\n  left: ["1", "3"]\n right: ["1", "2", "3"]'
        f = self.only("E-panic-r1", captured("step3_suites::plain_bug", "crates/persistence/tests/cassandra_tests.rs:300:9", message))
        self.assertEqual(f["class"], "other")
        self.assertIsNone(f["note"])
        self.assertEqual(f["first_line"], "assertion `left == right` failed: version history has a gap")

    # The delta audit's N1: a test that prints the result it did not expect,
    # `{other:?}`, prints the 504 inside Err(…) as its subject
    # (cassandra_tests.rs, "a repeated delete_versioned is NotFound: {other:?}").
    def test_n1_err_wrapped_timeout_is_issue_77(self) -> None:
        message = f"a repeated delete_versioned is NotFound: Err(Backend({SERIAL_READ_504}))"
        f = self.only("N1-err-r1", captured("delete_versioned_twice", "crates/persistence/tests/cassandra_tests.rs:558:14", message))
        self.assertEqual(f["class"], "504-77")
        self.assertIsNone(f["note"])

    def test_n1_got_err_unavailable_is_503(self) -> None:
        message = ('second seed without reset: Err(Backend(Unavailable { backend_name: "cassandra", '
                   'message: "Cassandra cannot serve the request right now" }))')
        f = self.only("N1b-r1", captured("seed_twice", "crates/persistence/tests/cassandra_tests.rs:448:14", message))
        self.assertEqual(f["class"], "503")

    # The `$` anchor: a line that goes on after the error is not about it
    # (the delta audit's H1), so it is "other", with a note.
    def test_subject_followed_by_more_text_is_other(self) -> None:
        message = loser(SERIAL_READ_504) + " instead of a refusal"
        f = self.only("H1-r1", captured("step3_suites::race_h1", RACE, message))
        self.assertEqual(f["class"], "other")
        self.assertIn("#77 504 in its message, not as its subject", f["note"])

    # The delta audit's N4 (a): a 503 or 504 in the configuration test keeps
    # its class; it is not "other, not the rack assertion".
    def test_n4_timeout_in_the_configuration_test_keeps_its_class(self) -> None:
        message = f"called `Result::unwrap()` on an `Err` value: Backend({SERIAL_READ_504})"
        f = self.only("N4-cfg-504-r1", captured("node_runs_the_docker_cassandra_configuration",
                                                "crates/persistence/tests/cassandra_tests.rs:178:6", message))
        self.assertEqual(f["class"], "504-77")

    def test_n4_unavailable_in_the_configuration_test_is_503(self) -> None:
        message = ('called `Result::unwrap()` on an `Err` value: Backend(Unavailable { backend_name: "cassandra", '
                   'message: "Cassandra cannot serve the request right now" })')
        f = self.only("N4-cfg-503-r1", captured("node_runs_the_docker_cassandra_configuration",
                                                "crates/persistence/tests/cassandra_tests.rs:178:6", message))
        self.assertEqual(f["class"], "503")

    # The delta audit's N4 (b): a test still running when the binary is
    # killed (an OOM kill, exit 137) is "killed", not "timed out".
    def test_n4_test_running_at_an_oom_kill_is_killed(self) -> None:
        text = """     Running tests/cassandra_tests.rs (/x/cassandra_tests-abc)

running 2 tests
test ok_test ... ok
test step3_suites::big has been running for over 60 seconds
"""
        f = self.only("N4-oom-r1", text, {"exit_status": 137, "timed_out": False, "killed": True, "oom_kills": 1})
        self.assertEqual(f["class"], "killed")
        self.assertIn("killed", f["first_line"])

    # G (audit B1): an invariant failure that prints every writer's outcome,
    # one of them a #77 504, is about the invariant.
    def test_g_invariant_failure_listing_a_504_is_other(self) -> None:
        message = ('round 3: the update won, but nothing is live: [Updated(StoredResource { id: "p" }), '
                   f"Failed(Backend({SERIAL_READ_504}))]")
        f = self.only("G-invariant-with-504-r1", captured("step3_suites::race_g",
                                                          "crates/persistence/tests/search/versioned_write_race_suite.rs:240:17", message))
        self.assertEqual(f["class"], "other")
        self.assertIn("#77 504 in its message, not as its subject", f["note"])

    def test_g2_no_write_landed_listing_a_504_is_other(self) -> None:
        message = ("round 1: no write landed, so no writer can have lost to one: "
                   f"[Failed(Backend({SERIAL_READ_504})), Failed(Backend({SERIAL_READ_504}))]")
        f = self.only("G2-r1", captured("step3_suites::race_g2", RACE, message))
        self.assertEqual(f["class"], "other")

    # F (audit S1), capture mode: the hung test is named as timed out, and a
    # test that failed before it, whose output libtest never printed, has an
    # unknown cause, not "other".
    def test_f_hung_test_in_capture_mode(self) -> None:
        text = """     Running tests/cassandra_tests.rs (/x/cassandra_tests-abc)

running 3 tests
test ok_test ... ok
test step3_suites::race_a ... FAILED
test step3_suites::hangs has been running for over 60 seconds
"""
        found = {f["test"]: f for f in self.failures("F-hung-r1", text, {"exit_status": 124, "timed_out": True, "wall_s": 480})}
        self.assertEqual(found["step3_suites::hangs"]["class"], "timed-out")
        self.assertEqual(found["step3_suites::race_a"]["class"], "unknown")

    # F with --nocapture, as the job runs: the failure before the hang keeps
    # its message and its cause.
    def test_f_hung_test_with_nocapture_keeps_earlier_messages(self) -> None:
        text = f"""     Running tests/cassandra_tests.rs (/x/cassandra_tests-abc)

running 4 tests
test ok_test ... ok

thread 'step3_suites::race_a' (162) panicked at {RACE}:
{loser(SERIAL_READ_504)}
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test step3_suites::race_a ... FAILED
Skipping nothing, a line a test printed
test step3_suites::hangs has been running for over 60 seconds
"""
        self.log("F2-hung-r1", text, {"exit_status": 124, "timed_out": True, "wall_s": 480})
        suite = report.parse_log(self.out / "logs" / "F2-hung-r1.log", stopped="timeout")
        found = {f["test"]: f for f in suite["failures"]}
        self.assertEqual(found["step3_suites::race_a"]["class"], "504-77")
        self.assertTrue(found["step3_suites::race_a"]["first_line"].startswith("round 0: a loser"))
        self.assertEqual(found["step3_suites::hangs"]["class"], "timed-out")
        # Four announced: ok_test, race_a and hangs reported, one never named.
        self.assertEqual(suite["unreported"], 1)

    # With --nocapture a finished run's failures are read from the inline panics.
    def test_nocapture_finished_run(self) -> None:
        text = f"""     Running tests/cassandra_tests.rs (/x/cassandra_tests-abc)

running 2 tests

thread 'step3_suites::race_a' (162) panicked at {RACE}:
{loser(SERIAL_READ_504)}
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test step3_suites::race_a ... FAILED
test ok_test ... ok

failures:

failures:
    step3_suites::race_a

test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s
"""
        f = self.only("nocapture-r1", text)
        self.assertEqual(f["class"], "504-77")
        self.assertEqual(f["location"], RACE)

    # The whole section: a timed-out run, an OOM-killed run (audit M2) and a
    # finished one, through cmd_tests.
    def test_section_reports_how_each_run_ended(self) -> None:
        self.log("cassandra-tests-r1", captured("step3_suites::race_a", RACE, loser(SERIAL_READ_504)),
                 {"exit_status": 101, "timed_out": False, "killed": False, "wall_s": 6, "oom_kills": 0})
        self.log("cassandra-tests-r2", """     Running tests/cassandra_tests.rs (/x/cassandra_tests-abc)

running 2 tests
test ok_test ... ok
test step3_suites::hangs has been running for over 60 seconds
""", {"exit_status": 124, "timed_out": True, "killed": False, "wall_s": 480, "oom_kills": 0})
        self.log("cassandra-tests-r3", """     Running tests/cassandra_tests.rs (/x/cassandra_tests-abc)

running 2 tests
test ok_test ... ok
""", {"exit_status": 137, "timed_out": False, "killed": True, "wall_s": 20, "oom_kills": 1})
        md, js = self.out / "f.md", self.out / "f.json"
        report.cmd_tests(str(self.out), str(md), str(js))
        data = json.loads(js.read_text())
        text = md.read_text()
        self.assertEqual(data["failures_by_class"]["504-77"], 1)
        self.assertEqual(data["failures_by_class"]["timed-out"], 1)
        self.assertEqual(data["runs_with_77"], {"cassandra-tests": 1})
        self.assertIn("| cassandra-tests-r2 |", text)
        self.assertIn("`step3_suites::hangs`", text)
        self.assertIn("| timeout |", text)
        self.assertIn("| killed (OOM kill) |", text)
        self.assertEqual(report.ended({"exit_status": 137, "killed": True, "oom_kills": 1}), "killed (OOM kill)")
        self.assertEqual(report.ended({"exit_status": 137, "killed": True, "oom_kills": 0}), "killed")
        self.assertEqual(report.ended({"exit_status": 124, "timed_out": True}), "timeout")
        self.assertEqual(report.ended({"exit_status": 101}), "exit")

    # CodeRabbit on #101: no log at all (the schema step failed, so both test
    # steps were skipped) is not "No test failed".
    def test_no_logs_is_no_test_ran(self) -> None:
        md, js = self.out / "f.md", self.out / "f.json"
        report.cmd_tests(str(self.out), str(md), str(js))
        self.assertNotIn("No test failed", md.read_text())
        self.assertIn("No test ran", md.read_text())
        self.assertTrue(any("no test ran" in w for w in json.loads(js.read_text())["warnings"]))

    # CodeRabbit on #101: a repeat run-tests.sh did not start (too close to
    # the step's deadline) is listed as not run and is not counted as a run.
    def test_deadline_skipped_repeat_is_listed_and_not_counted(self) -> None:
        self.log("cassandra-tests-r1", captured("step3_suites::race_a", RACE, loser(SERIAL_READ_504)),
                 {"exit_status": 101, "timed_out": False, "killed": False, "wall_s": 6, "oom_kills": 0})
        self.log("cassandra-tests-r2", "     Running tests/cassandra_tests.rs (/x/cassandra_tests-abc)\n"
                 "not run (run-tests.sh): 40 s left before the step's deadline\n",
                 {"skipped": "deadline", "left_s": 40})
        md, js = self.out / "f.md", self.out / "f.json"
        report.cmd_tests(str(self.out), str(md), str(js))
        data = json.loads(js.read_text())
        self.assertEqual(data["runs"], {"cassandra-tests": 1})
        self.assertIn("| cassandra-tests-r2 |", md.read_text())
        self.assertIn("| not run (deadline) |", md.read_text())
        self.assertIn("1 of 1 runs", md.read_text())
        self.assertTrue(any("cassandra-tests-r2 not run (deadline: 40 s" in w for w in data["warnings"]))
        self.assertFalse(any("printed no `test result` line" in w for w in data["warnings"]))

    def test_every_repeat_skipped_is_no_test_ran(self) -> None:
        self.log("cassandra-tests-r1", "     Running tests/cassandra_tests.rs (/x/cassandra_tests-abc)\n",
                 {"skipped": "deadline", "left_s": 10})
        md, js = self.out / "f.md", self.out / "f.json"
        report.cmd_tests(str(self.out), str(md), str(js))
        self.assertIn("No test ran", md.read_text())
        self.assertEqual(json.loads(js.read_text())["runs"], {})

    # CodeRabbit on #101: since PR #83 a race test accepts a #77 504 and
    # passes, so the report counts the write engine's failed serial reads
    # from the tests' [cas] lines (run 44758590016's first pass had 6).
    def test_failed_serial_reads_are_counted_in_passing_runs(self) -> None:
        cas = ("[cas case=race:cassandra_concurrent_update_and_plain_delete_stay_consistent attempts=95 applied=23 "
               "unknown_write_timeout_cas=10 serial_reads=11 serial_read_failures=3 undecidable=0]\n"
               "[cas case=race:cassandra_concurrent_updates_from_the_same_version_admit_one attempts=90 applied=20 "
               "serial_reads=12 serial_read_failures=1 undecidable=0]\n")
        passing = ("     Running tests/cassandra_tests.rs (/x/cassandra_tests-abc)\n\nrunning 2 tests\n"
                   "[race policy case=race:cassandra_concurrent_updates_from_the_same_version_admit_one rf=3 accept_504=true]\n"
                   + cas + "test a ... ok\ntest b ... ok\n\n"
                   "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s\n")
        self.log("cassandra-tests-r1", passing, {"exit_status": 0})
        self.log("cassandra-tests-r2", passing.replace("serial_read_failures=3", "serial_read_failures=0"),
                 {"exit_status": 0})
        md, js = self.out / "f.md", self.out / "f.json"
        report.cmd_tests(str(self.out), str(md), str(js))
        data = json.loads(js.read_text())
        self.assertEqual(data["serial_read_failures"], {"cassandra-tests": [4, 1]})
        self.assertEqual(data["runs_with_77"], {})
        self.assertIn("`cassandra-tests` 4, 1 (5 in 2 runs)", md.read_text())
        self.assertIn("No test failed", md.read_text())
        self.assertEqual(data["accepting_uncounted"], [])

    # CodeRabbit on #101: the create race accepts a 504 and prints no [cas]
    # line (create_read.rs), so the report names it as uncounted.
    def test_accepting_race_without_cas_line_is_named(self) -> None:
        self.log("cassandra-tests-r1", "     Running tests/cassandra_tests.rs (/x/cassandra_tests-abc)\n\n"
                 "running 2 tests\n"
                 "[race policy case=race:cassandra_concurrent_creates_of_one_id_make_exactly_one_resource rf=3 accept_504=true]\n"
                 "[race policy case=race:cassandra_concurrent_updates_from_the_same_version_admit_one rf=3 accept_504=true]\n"
                 "[cas case=race:cassandra_concurrent_updates_from_the_same_version_admit_one serial_read_failures=2]\n"
                 "test a ... ok\ntest b ... ok\n\n"
                 "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s\n",
                 {"exit_status": 0})
        md, js = self.out / "f.md", self.out / "f.json"
        report.cmd_tests(str(self.out), str(md), str(js))
        data = json.loads(js.read_text())
        self.assertEqual(data["accepting_uncounted"], ["cassandra_concurrent_creates_of_one_id_make_exactly_one_resource"])
        self.assertIn("print no `[cas]` line", md.read_text())

    # CodeRabbit on #101: the fallback writes never raise, and main exits 0
    # when the JSON destination cannot be written.
    def test_report_exits_zero_when_a_destination_is_unwritable(self) -> None:
        import contextlib
        import io
        import sys
        md, js = self.out / "f.md", self.out / "missing" / "f.json"
        argv, sys.argv = sys.argv, ["report.py", "tests", str(self.out), str(md), str(js)]
        try:
            with contextlib.redirect_stdout(io.StringIO()) as printed:
                self.assertEqual(report.main(), 0)
        finally:
            sys.argv = argv
        self.assertIn("The report could not be built", md.read_text())
        self.assertIn("could not write", printed.getvalue())


# The pause scenarios, with the outputs of run 44758590016 (2026-11-20) as
# the shape: cqlsh prints "Consistency level set" first, an error on the
# line of the failing statement, and the read at ALL as SELECT JSON rows.
CONSISTENCY = "Consistency level set to LOCAL_QUORUM.\n"
OUT = {
    "s1": CONSISTENCY,
    "s2": "<stdin>:6:WriteTimeout: Error from server: code=1100 [Coordinator node timed out waiting for replica "
          "nodes' responses] message=\"Operation timed out - received only 1 responses.\" info={'consistency': "
          "'LOCAL_QUORUM', 'required_responses': 2, 'received_responses': 1, 'write_type': 'SIMPLE'}\n" + CONSISTENCY,
    "s3": "<stdin>:6:NoHostAvailable: ('Unable to complete the operation against any hosts', {<Host: "
          "127.0.0.1:9042 helios-dc1>: Unavailable('Error from server: code=1000 [Unavailable exception] "
          "message=\"Cannot achieve consistency level LOCAL_QUORUM\" info={\\'consistency\\': "
          "\\'LOCAL_QUORUM\\', \\'required_replicas\\': 2, \\'alive_replicas\\': 1}')})\n" + CONSISTENCY,
    "s4a": "<stdin>:6:WriteTimeout: Error from server: code=1100 [Coordinator node timed out waiting for replica "
           "nodes' responses] message=\"Operation timed out - received only 1 responses.\" info={'consistency': "
           "'TWO', 'required_responses': 2, 'received_responses': 1, 'write_type': 'BATCH_LOG'}\n" + CONSISTENCY,
    "s4b": CONSISTENCY,
}
P2 = '{"resource_type": "Patient", "lu": 2000, "id": "p", "version_id": 2, "w": 2000001}'
P1 = '{"resource_type": "Patient", "lu": 1000, "id": "p", "version_id": 1, "w": 1000001}'
P1NEW = '{"resource_type": "Patient", "lu": 2000, "id": "p", "version_id": 1, "w": 2000001}'
O1 = '{"resource_type": "Observation", "lu": 2000, "id": "o", "version_id": 1, "w": 2000001}'
READ = {"s1": [P2], "s2": [P2], "s3": [P1], "s4a": [O1, P1NEW], "s4b": [O1, P1NEW]}
KIND = {"s1": "one", "s2": "one", "s3": "one", "s4a": "two", "s4b": "two"}


class Pause(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        for sid in OUT:
            (self.dir / f"{sid}.meta").write_text(f"id={sid}\nkind={KIND[sid]}\nstate=UN (at once)\ntook_ms=1\n")
            (self.dir / f"{sid}.out").write_text(OUT[sid])
            rows = "".join(f" {r}\n" for r in READ[sid])
            (self.dir / f"{sid}.read").write_text(f"Consistency level set to ALL.\n\n [json]\n---\n{rows}\n"
                                                  f"({len(READ[sid])} rows)\n")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def run_pause(self) -> tuple[dict, str, list[str]]:
        md, js = self.dir / "p.md", self.dir / "p.json"
        report.cmd_pause(str(self.dir), str(md), str(js))
        data = json.loads(js.read_text())
        return {s["id"]: s for s in data["scenarios"]}, md.read_text(), data["warnings"]

    def test_run_44758590016_is_as_expected(self) -> None:
        scenarios, _, warnings = self.run_pause()
        self.assertEqual({sid: s["as_expected"] for sid, s in scenarios.items()},
                         {sid: True for sid in OUT})
        self.assertEqual(warnings, [])

    # CodeRabbit on #101: the answer's counts are compared, not only its
    # type (s2: one response of two in the review's run).
    def test_changed_counts_differ(self) -> None:
        (self.dir / "s2.out").write_text(OUT["s2"].replace("'received_responses': 1", "'received_responses': 0"))
        scenarios, md, warnings = self.run_pause()
        self.assertFalse(scenarios["s2"]["as_expected"])
        self.assertTrue(scenarios["s4a"]["as_expected"])
        self.assertIn("WriteTimeout SIMPLE, received_responses 1, required_responses 2, applied yes", md)
        self.assertTrue(any("s2 differs" in w and "received_responses 0" in w for w in warnings))

    # CodeRabbit on #101: a read cut off after a whole row is unread, not
    # the rows it kept.
    def test_truncated_read_is_unread(self) -> None:
        (self.dir / "s1.read").write_text(f"Consistency level set to ALL.\n\n [json]\n---\n {P2}\n {P1[:30]}\n")
        scenarios, md, warnings = self.run_pause()
        self.assertEqual(scenarios["s1"]["applied"], "unread")
        self.assertIsNone(scenarios["s1"]["as_expected"])
        self.assertTrue(any("s1: its read at ALL is missing or incomplete" in w for w in warnings))
        self.assertIsNone(report.parse_rows(f" {P2}\n\n(2 rows)\n"))
        self.assertEqual(report.parse_rows(" [json]\n---\n\n(0 rows)\n"), [])

    # CodeRabbit on #101: no output file is unknown, not success.
    def test_missing_output_is_unknown(self) -> None:
        (self.dir / "s1.out").unlink()
        scenarios, md, warnings = self.run_pause()
        self.assertEqual(scenarios["s1"]["outcome"], "unknown")
        self.assertIsNone(scenarios["s1"]["as_expected"])
        self.assertIn("| unknown |", md)
        self.assertTrue(any("s1: its answer is unknown (no cqlsh output file)" in w for w in warnings))

    def test_empty_output_is_unknown(self) -> None:
        (self.dir / "s4b.out").write_text("")
        scenarios, _, warnings = self.run_pause()
        self.assertEqual(scenarios["s4b"]["outcome"], "unknown")
        self.assertIsNone(scenarios["s4b"]["as_expected"])

    # CodeRabbit on #101: a batch sent "at once" after which node 1 already
    # reports the paused node DN is invalid, not compared.
    def test_at_once_batch_without_un_is_invalid(self) -> None:
        with (self.dir / "s4a.meta").open("a") as f:
            f.write("seen_after=10.59.142.12 DN\ninvalid=node 1 reported 10.59.142.12 DN right after the batch\n")
        scenarios, md, warnings = self.run_pause()
        self.assertIsNone(scenarios["s4a"]["as_expected"])
        self.assertIn("UN (at once); 10.59.142.12 DN right after", md)
        self.assertIn("| invalid |", md)
        self.assertTrue(any(w.startswith("Pause scenario s4a is invalid") for w in warnings))
        self.assertFalse(any("s4a differs" in w for w in warnings))

    # CodeRabbit on #101: a DN batch the script did not send (node 1 never
    # reported the node DN) is not run.
    def test_dn_batch_not_sent_is_not_run(self) -> None:
        (self.dir / "s4b.out").unlink()
        (self.dir / "s4b.meta").write_text("id=s4b\nkind=two\nstate=DN\n"
                                           "not_run=node 1 did not report node 2 DN within the wait\n")
        scenarios, md, warnings = self.run_pause()
        self.assertEqual(scenarios["s4b"]["outcome"], "not run")
        self.assertIsNone(scenarios["s4b"]["as_expected"])
        self.assertIn("| not run | node 1 did not report node 2 DN within the wait |", md)
        self.assertTrue(any(w.startswith("Pause scenario s4b not run") for w in warnings))


if __name__ == "__main__":
    unittest.main()
