#!/usr/bin/env python3
"""Tests for mutants-report.py, the report of the `mutants` job in
.github/workflows/cassandra-mutants.yml. Standard library only, as
test_ci_report.py:

    PYTHONPATH=.github/scripts python3 -m unittest -v test_mutants_report

The outcomes below have the shape cargo-mutants 27.1.0 writes to
mutants.out/outcomes.json, taken from a real run on a small crate (a
baseline, then one caught, one missed, one unviable and one timed-out
mutant), with the names and files changed to the backend's.
"""

from __future__ import annotations

import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("mutants_report", HERE / "mutants-report.py")
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)

FILE = "crates/persistence/src/backends/cassandra/batch.rs"


def mutant(line: int, col: int, what: str, function: str | None, replacement: str) -> dict:
    return {
        "name": f"{FILE}:{line}:{col}: {what}",
        "package": "helios-persistence",
        "file": FILE,
        "function": None if function is None else {
            "function_name": function,
            "return_type": "-> bool",
            "span": {"start": {"line": line - 2, "column": 1}, "end": {"line": line + 5, "column": 2}},
        },
        "span": {"start": {"line": line, "column": col}, "end": {"line": line, "column": col + 2}},
        "replacement": replacement,
        "genre": "BinaryOperator",
    }


def outcome(summary: str, scenario, build: float | None, test: float | None, test_status="Success") -> dict:
    phases = []
    if build is not None:
        phases.append({"phase": "Build", "duration": build,
                       "process_status": "Success" if summary != "Unviable" else {"Failure": 101},
                       "argv": ["cargo", "test", "--no-run"]})
    if test is not None:
        phases.append({"phase": "Test", "duration": test, "process_status": test_status, "argv": ["cargo", "test"]})
    name = "baseline" if scenario == "Baseline" else scenario["Mutant"]["name"].replace("/", "__")
    return {"scenario": scenario, "summary": summary, "log_path": f"log/{name}.log",
            "diff_path": None if scenario == "Baseline" else f"diff/{name}.diff", "phase_results": phases}


CAUGHT = {"Mutant": mutant(240, 20, "replace + with - in pack", "pack", "-")}
MISSED = {"Mutant": mutant(131, 51, "replace || with && in has_at", "has_at", "&&")}
UNVIABLE = {"Mutant": mutant(98, 1, "replace writes_an_index_table -> bool with Default::default()", "writes_an_index_table",
                             "Default::default()")}
TIMEOUT = {"Mutant": mutant(256, 9, "replace -= with /= in pack", "pack", "/=")}


def outcomes(*items, baseline="Success") -> dict:
    listed = [outcome(baseline, "Baseline", 210.0, 1.5)] + list(items)
    count = {k: sum(1 for o in items if o["summary"] == s) for k, s in
             (("caught", "CaughtMutant"), ("missed", "MissedMutant"), ("unviable", "Unviable"), ("timeout", "Timeout"))}
    return {"outcomes": listed, "total_mutants": len(items), **count, "success": 0,
            "start_time": "2026-11-20T05:01:40Z", "end_time": "2026-11-20T05:31:40Z", "cargo_mutants_version": "27.1.0"}


ALL = [
    outcome("CaughtMutant", CAUGHT, 30.0, 2.0, {"Failure": 101}),
    outcome("MissedMutant", MISSED, 40.0, 1.0),
    outcome("Unviable", UNVIABLE, 5.0, None),
    outcome("Timeout", TIMEOUT, 35.0, 20.0, "Timeout"),
]


class Run(unittest.TestCase):
    def write(self, outcomes_json: dict | None, mutants: list | None) -> Path:
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        out = tmp / "mutants.out"
        out.mkdir()
        if outcomes_json is not None:
            (out / "outcomes.json").write_text(json.dumps(outcomes_json))
        if mutants is not None:
            (out / "mutants.json").write_text(json.dumps(mutants))
        return out


class Counts(Run):
    def test_counts_score_and_the_missed_table(self):
        out = self.write(outcomes(*ALL), [CAUGHT["Mutant"], MISSED["Mutant"], UNVIABLE["Mutant"], TIMEOUT["Mutant"]])
        data = report.build(out, 2, None)
        self.assertEqual(data["counts"], {"caught": 1, "missed": 1, "unviable": 1, "timeout": 1})
        self.assertEqual(data["generated"], 4)
        self.assertEqual(data["tested"], 4)
        # Caught over caught + missed + timed out; unviable is not viable.
        self.assertEqual(data["score"], 0.333)
        self.assertEqual([m["line"] for m in data["missed"]], [131])
        self.assertEqual(data["missed"][0]["function"], "has_at")
        self.assertEqual(data["missed"][0]["mutation"], "replace || with && in has_at")
        self.assertEqual(data["baseline"]["summary"], "Success")
        self.assertEqual(data["build_times"], {"n": 4, "median_s": 32.5, "max_s": 40.0, "total_s": 110.0})
        self.assertEqual(data["test_times"]["n"], 3)
        self.assertEqual(data["warnings"], [])
        self.assertEqual(data["exit_meaning"], "some mutants were missed")

    def test_markdown_escapes_pipes_and_names_file_and_line(self):
        out = self.write(outcomes(*ALL), None)
        md = report.markdown(report.build(out, 3, None))
        self.assertIn("| 4 | 1 | 1 | 1 | 1 | 33.3% |", md)
        self.assertIn(f"`{FILE}:131`", md)
        # A `||` inside a cell would split the row into more columns.
        self.assertIn("replace \\|\\| with && in has_at", md)
        for row in md.splitlines():
            if row.startswith("| `crates"):
                self.assertEqual(row.replace("\\|", "").count("|"), row.count("|") - row.count("\\|"))
        self.assertIn("### Timed-out mutants", md)
        self.assertIn("replace -= with /= in pack", md)
        self.assertIn("### Per file", md)
        self.assertIn("Baseline (unmutated)", md)

    def test_every_mutant_caught_has_no_missed_section(self):
        out = self.write(outcomes(ALL[0]), [CAUGHT["Mutant"]])
        data = report.build(out, 0, None)
        self.assertEqual(data["score"], 1.0)
        self.assertNotIn("### Missed mutants", report.markdown(data))


class Failures(Run):
    def test_a_failed_baseline_is_a_warning(self):
        out = self.write(outcomes(baseline="Failure"), [CAUGHT["Mutant"]])
        data = report.build(out, 4, None)
        self.assertTrue(any("baseline" in w for w in data["warnings"]))
        self.assertTrue(any("1 of 1 mutants were not tested" in w for w in data["warnings"]))
        self.assertIsNone(data["score"])

    def test_a_run_cut_short_counts_the_untested_mutants(self):
        out = self.write(outcomes(ALL[0], ALL[1]), [m["scenario"]["Mutant"] for m in ALL])
        data = report.build(out, None, None)
        self.assertEqual(data["untested"], 2)

    def test_no_outcomes_with_an_empty_diff_is_a_note_not_a_warning(self):
        out = self.write(None, None)
        diff = out.parent / "pr.diff"
        diff.write_text("")
        data = report.build(out, 0, diff)
        self.assertFalse(data["outcomes_read"])
        self.assertEqual(data["warnings"], [])
        self.assertIn("no mutants", data["note"])
        self.assertIn("no mutants", report.markdown(data))

    def test_no_outcomes_after_a_usage_error_warns(self):
        out = self.write(None, None)
        data = report.build(out, 1, None)
        self.assertTrue(any("exited 1 (usage error)" in w for w in data["warnings"]))

    def test_an_outcomes_document_of_the_wrong_shape_warns_and_reads_nothing(self):
        for doc in ([], None, "x", {"outcomes": {"a": 1}}, {}, {"outcomes": None}):
            tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
            out = tmp / "mutants.out"
            out.mkdir()
            (out / "outcomes.json").write_text(json.dumps(doc))
            data = report.build(out, 0, None)
            self.assertFalse(data["outcomes_read"], doc)
            self.assertTrue(any("not an outcomes document" in w for w in data["warnings"]), doc)
            self.assertIn("not an outcomes document", report.markdown(data))
            dest = tmp / "report"
            with redirect_stdout(io.StringIO()):
                self.assertEqual(report.main([str(out), str(dest), "--exit-status", "0"]), 0)
            self.assertTrue((dest / "mutants.md").exists() and (dest / "mutants.json").exists())

    def test_an_empty_outcomes_array_is_read(self):
        doc = outcomes()
        doc["outcomes"] = []
        data = report.build(self.write(doc, None), 0, None)
        self.assertTrue(data["outcomes_read"])
        self.assertFalse(any("not an outcomes document" in w for w in data["warnings"]))

    def test_an_outcome_that_is_not_an_object_is_skipped_and_the_rest_counted(self):
        doc = outcomes(*ALL)
        doc["outcomes"].append(["not", "an", "object"])
        data = report.build(self.write(doc, None), 2, None)
        self.assertEqual(data["counts"], {"caught": 1, "missed": 1, "unviable": 1, "timeout": 1})
        self.assertTrue(any("not an object (a JSON array)" in w for w in data["warnings"]))

    def test_an_unknown_outcome_is_reported_not_counted(self):
        odd = outcome("Weird", CAUGHT, 1.0, 1.0)
        out = self.write(outcomes(odd), None)
        data = report.build(out, 0, None)
        self.assertEqual(sum(data["counts"].values()), 0)
        self.assertTrue(any("unknown outcome 'Weird'" in w for w in data["warnings"]))


class Diff(Run):
    def test_diff_stats(self):
        out = self.write(outcomes(*ALL), None)
        diff = out.parent / "pr.diff"
        diff.write_text(f"diff --git a/{FILE} b/{FILE}\n--- a/{FILE}\n+++ b/{FILE}\n@@ -1,2 +1,3 @@\n a\n-b\n+c\n+d\n")
        data = report.build(out, 2, diff)
        self.assertEqual(data["diff"], {"files": [FILE], "added_lines": 2, "removed_lines": 1})
        self.assertIn("Diff: 1 file(s) under the backend, +2 / -1 lines.", report.markdown(data))


class Main(Run):
    def test_main_writes_both_files_and_exits_0(self):
        out = self.write(outcomes(*ALL), None)
        dest = out.parent / "report"
        with redirect_stdout(io.StringIO()):
            status = report.main([str(out), str(dest), "--exit-status", "2", "--title-note", "PR #90"])
        self.assertEqual(status, 0)
        self.assertTrue((dest / "mutants.md").read_text().startswith("PR #90"))
        self.assertEqual(json.loads((dest / "mutants.json").read_text())["exit_status"], 2)

    def test_main_with_nothing_to_read_still_exits_0(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        with redirect_stdout(io.StringIO()):
            self.assertEqual(report.main([str(tmp / "missing"), str(tmp / "out"), "--exit-status", "70"]), 0)


if __name__ == "__main__":
    unittest.main()
