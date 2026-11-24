#!/usr/bin/env python3
"""Tests for mutants-report.py, the report of the `mutants` job in
.github/workflows/cassandra-mutants.yml, and for mutants-result-alias.py,
which lets cargo-mutants see through `StorageResult<T>` in the job's
checkout. Standard library only, as test_ci_report.py:

    PYTHONPATH=.github/scripts python3 -m unittest -v test_mutants_report

The outcomes below have the shape cargo-mutants 27.1.0 writes to
mutants.out/outcomes.json, taken from a real run on a small crate (a
baseline, then one caught, one missed, one unviable and one timed-out
mutant), with the names and files changed to the backend's.
"""

from __future__ import annotations

import contextlib
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
alias_spec = importlib.util.spec_from_file_location("mutants_result_alias", HERE / "mutants-result-alias.py")
alias = importlib.util.module_from_spec(alias_spec)
alias_spec.loader.exec_module(alias)

STORAGE = {"StorageResult": "crate::error::StorageError"}

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


def unviable(line: int, what: str, replacement: str, error: str | None) -> tuple[dict, str | None]:
    """An unviable mutant's outcome, and the log its build left (None: no log written)."""
    m = {"Mutant": mutant(line, 9, what, "f", replacement)}
    log = None if error is None else ("\x1b[1m\x1b[92m   Compiling\x1b[0m helios-persistence v0.2.9\n"
                                      f"\x1b[0m\x1b[1m\x1b[91m{error}\x1b[0m\n"
                                      "\x1b[1m\x1b[91merror\x1b[0m: could not compile `helios-persistence`\n")
    return outcome("Unviable", m, 7.0, None), log


ALIAS_ERROR = "error[E0599]: no variant, associated function, or constant named `new` found for enum `Result<T, E>`"
CONST_ERROR = "error[E0080]: evaluation panicked: an index batch takes only an INSERT INTO or a DELETE FROM"
DEFAULT_ERROR = "error[E0277]: the trait bound `BackendCapability: Default` is not satisfied"


class Unviable(Run):
    def write_logs(self, out: Path, items: list[tuple[dict, str | None]]) -> None:
        (out / "log").mkdir(exist_ok=True)
        for item, log in items:
            if log is not None:
                (out / item["log_path"]).write_text(log)

    def run_of(self, items: list[tuple[dict, str | None]], *viable: dict) -> dict:
        out = self.write(outcomes(*viable, *(i for i, _ in items)), None)
        self.write_logs(out, items)
        return report.build(out, 0, None)

    def test_an_alias_run_tested_little_and_says_why(self):
        # Run 44801901198's shape: one caught, and all the rest StorageResult alias mutants.
        items = [unviable(200 + i, f"replace f -> StorageResult<Option<String>> with StorageResult::{c}(None)",
                          f"StorageResult::{c}(None)", ALIAS_ERROR) for i, c in enumerate(("new", "from", "from_iter"))]
        data = self.run_of(items, ALL[0])
        self.assertTrue(data["tested_little"])
        self.assertEqual((data["untestable"], data["untestable_share"]), (3, 0.75))
        [reason] = data["unviable_reasons"]
        self.assertEqual(reason["count"], 3)
        self.assertIn("`StorageResult::new`", reason["reason"])
        self.assertIn("does not see through", reason["reason"])
        self.assertEqual(data["unviable"][0]["rustc_error"]["code"], "E0599")
        [warning] = data["warnings"]
        self.assertIn("tested little: 3 of 4 mutants (75%)", warning)
        self.assertIn("StorageResult::new", warning)
        md = report.markdown(data)
        self.assertTrue(md.startswith("> **Warning: this run tested little.** 3 of 4 mutants (75%)"))
        self.assertIn("### Why the unviable mutants did not compile", md)

    def test_const_eval_rejections_are_as_good_as_caught_and_do_not_warn(self):
        items = [unviable(101 + i, f"replace < with > in writes_an_index_table #{i}", ">", CONST_ERROR) for i in range(3)]
        items.append(unviable(120, "replace += with *= in has_at", "*=",
                              "error: constant evaluation is taking a long time"))
        data = self.run_of(items, ALL[0])
        self.assertFalse(data["tested_little"])
        self.assertEqual(data["untestable"], 0)
        [reason] = data["unviable_reasons"]
        self.assertTrue(reason["const_eval"])
        self.assertEqual(reason["count"], 4)
        self.assertEqual(data["warnings"], [])
        self.assertNotIn("tested little", report.markdown(data))

    def test_half_untestable_warns_and_less_does_not(self):
        default = unviable(438, "replace f -> Vec<BackendCapability> with vec![Default::default()]",
                           "vec![Default::default()]", DEFAULT_ERROR)
        self.assertTrue(self.run_of([default], ALL[0])["tested_little"])
        data = self.run_of([default], ALL[0], ALL[1])
        self.assertFalse(data["tested_little"])
        self.assertEqual(data["unviable_reasons"][0]["reason"], "`Default::default()` for a type with no `Default` impl")

    def test_other_reasons(self):
        items = [
            unviable(243, "replace pack -> Vec<Range<usize>> with vec![Range::new(0)]", "vec![Range::new(0)]",
                     "error[E0599]: no associated function or constant named `new` found for struct `Range`"),
            unviable(244, "replace pack -> Vec<Range<usize>> with vec![Range::from(0)]", "vec![Range::from(0)]",
                     "error[E0277]: the trait bound `Range<usize>: From<{integer}>` is not satisfied"),
            unviable(2198, "replace + with * in g", "*", "error[E0369]: cannot multiply `Duration` by `Duration`"),
            unviable(2199, "replace - with + in g", "+", None),
        ]
        data = self.run_of(items, *ALL[:2], ALL[0], ALL[0], ALL[0], ALL[0])
        reasons = {r["reason"]: r["count"] for r in data["unviable_reasons"]}
        self.assertEqual(reasons, {
            "a guessed constructor the type does not have (`T::new`, `::from` or `::from_iter`)": 2,
            "rustc E0369: cannot multiply `…` by `…`": 1,
            "no rustc error found in the mutant's log": 1,
        })
        self.assertFalse(data["tested_little"])

    def test_a_log_path_that_is_not_a_relative_path_inside_mutants_out_is_not_read(self):
        # outcomes.json is the pull request's run output: a log_path that is not a
        # string, or that leaves mutants.out, must neither crash the report nor
        # bring another file's first error line into the step summary.
        for bad in (["log/x.log"], 7, [], 0, "", "missing", "/etc/passwd", "../secret.log", "log/../../secret.log"):
            with self.subTest(log_path=bad):
                item, _ = unviable(438, "replace f -> u8 with Default::default()", "Default::default()", None)
                if bad == "missing":
                    del item["log_path"]
                else:
                    item["log_path"] = bad
                out = self.write(outcomes(item), None)
                (out.parent / "secret.log").write_text("error[E0001]: SECRET\n")
                data = report.build(out, 0, None)
                self.assertIsNone(data["unviable"][0]["rustc_error"])
                self.assertTrue(any("is not a relative path inside mutants.out" in w for w in data["warnings"]))
                dest = out.parent / "report"
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(report.main([str(out), str(dest)]), 0)
                self.assertNotIn("SECRET", (dest / "mutants.md").read_text())

    def test_first_rustc_error_skips_the_summary_lines(self):
        log = ("\x1b[91merror\x1b[0m: aborting due to 2 previous errors\n"
               "\x1b[1m\x1b[91merror[E0283]\x1b[0m\x1b[1m: type annotations needed\x1b[0m\n")
        self.assertEqual(report.first_rustc_error(log), ("E0283", "type annotations needed"))
        self.assertIsNone(report.first_rustc_error("warning: unused import\n"))


class Unalias(Run):
    def test_rewritten_names_are_spelled_as_the_source(self):
        name = (f"{FILE}:200:9: replace current_version -> ::core::result::Result<Option<Vec<String>>, "
                "crate::error::StorageError> with Ok(None)")
        self.assertEqual(report.unalias(name, STORAGE),
                         f"{FILE}:200:9: replace current_version -> StorageResult<Option<Vec<String>>> with Ok(None)")
        other = "replace f -> ::core::result::Result<u8, Other> with Ok(0)"
        self.assertEqual(report.unalias(other, STORAGE), other)
        self.assertEqual(report.unalias(name, {}), name)

    def test_build_and_main_unalias_the_names(self):
        m = {"Mutant": mutant(200, 9, "replace f -> ::core::result::Result<(), crate::error::StorageError> with Ok(())",
                              "f", "Ok(())")}
        out = self.write(outcomes(outcome("MissedMutant", m, 30.0, 1.0)), None)
        self.assertEqual(report.build(out, 2, None, STORAGE)["missed"][0]["mutation"],
                         "replace f -> StorageResult<()> with Ok(())")
        dest = out.parent / "report"
        with redirect_stdout(io.StringIO()):
            report.main([str(out), str(dest), "--alias", "StorageResult=crate::error::StorageError"])
        self.assertIn("replace f -> StorageResult<()> with Ok(())", (dest / "mutants.md").read_text())


class ResultAlias(unittest.TestCase):
    def rewrite(self, text: str) -> str:
        new, _ = alias.rewrite_source(text, STORAGE)
        self.assertEqual(new.count("\n"), text.count("\n"))
        return new

    def test_a_return_type_is_spelled_as_a_result(self):
        self.assertEqual(self.rewrite("fn f() -> StorageResult<Option<String>> {\n"),
                         "fn f() -> ::core::result::Result<Option<String>, crate::error::StorageError> {\n")
        self.assertEqual(self.rewrite("fn f() -> crate::error::StorageResult<()> {"),
                         "fn f() -> ::core::result::Result<(), crate::error::StorageError> {")

    def test_multi_line_nested_and_arrows_inside(self):
        text = ("fn f(\n) -> StorageResult<\n    Vec<(String, Box<dyn Fn() -> u8>)>,\n> {\n"
                "fn g() -> StorageResult<Vec<StorageResult<u8>>> {}\n")
        self.assertEqual(self.rewrite(text),
                         "fn f(\n) -> ::core::result::Result<\n    Vec<(String, Box<dyn Fn() -> u8>)>,\n"
                         " crate::error::StorageError> {\n"
                         "fn g() -> ::core::result::Result<Vec<StorageResult<u8>>, crate::error::StorageError> {}\n")

    def test_only_return_types_in_code_are_rewritten(self):
        text = ('use crate::error::StorageResult;\n'
                '/// fn f() -> StorageResult<T> is documented, /* -> StorageResult< */\n'
                'let s = "-> StorageResult<"; let c = \'"\'; let r = r#"-> StorageResult<"#;\n'
                'let x: StorageResult<()> = Ok(()); fn h<\'a>(v: &\'a str) -> StorageResult<&\'a str> {}\n')
        new, count = alias.rewrite_source(text, STORAGE)
        self.assertEqual(count, 1)
        self.assertEqual(new, text.replace("-> StorageResult<&'a str>",
                                           "-> ::core::result::Result<&'a str, crate::error::StorageError>"))
        self.assertEqual(alias.rewrite_source("fn f() -> SearchResult<u8> {}", STORAGE), ("fn f() -> SearchResult<u8> {}", 0))

    def test_a_raw_string_with_a_quote_inside_is_not_code(self):
        # A plain-string scan would end at the inner `"` and read the rest as code.
        text = 'let r = r#"a " -> StorageResult<u8> b"#;\nlet b = br"-> StorageResult<";\n'
        self.assertEqual(alias.rewrite_source(text, STORAGE), (text, 0))

    def test_brackets_semicolons_and_const_blocks_inside_the_generics(self):
        for inner in ("[u8; 4]", "Foo<{ N }>", "Foo<u8, { N + 1 }>", "(Vec<u8>, [u8; 2])", "Box<dyn Fn(u8) -> u8>"):
            with self.subTest(inner=inner):
                self.assertEqual(self.rewrite(f"fn f() -> StorageResult<{inner}> {{\n}}\n"),
                                 f"fn f() -> ::core::result::Result<{inner}, crate::error::StorageError> {{\n}}\n")

    def test_a_comment_after_or_ending_with_a_comma_is_not_the_trailing_comma(self):
        self.assertEqual(self.rewrite("fn f() -> StorageResult<u8, /* note */> {}"),
                         "fn f() -> ::core::result::Result<u8, /* note */ crate::error::StorageError> {}")
        self.assertEqual(self.rewrite("fn f() -> StorageResult<\n    u8 // a note,\n> {}"),
                         "fn f() -> ::core::result::Result<\n    u8 // a note,\n, crate::error::StorageError> {}")
        self.assertEqual(self.rewrite("fn f() -> StorageResult<u8 /* a, */> {}"),
                         "fn f() -> ::core::result::Result<u8 /* a, */, crate::error::StorageError> {}")

    def test_an_unbalanced_bracket_is_refused(self):
        with self.assertRaises(alias.Unbalanced):
            alias.rewrite_source("fn f() -> StorageResult<u8 {\n}\n", STORAGE)

    def test_the_diff_follows_the_rewritten_file(self):
        path = "crates/persistence/src/backends/cassandra/x.rs"
        diff = (f"diff --git a/{path} b/{path}\n--- a/{path}\n+++ b/{path}\n@@ -1,3 +1,4 @@\n"
                " fn f() -> StorageResult<u8> {\n-    old\n+    new\n+++ looks like a header\n }\n"
                "\\ No newline at end of file\n")
        rewritten = ["fn f() -> ::core::result::Result<u8, crate::error::StorageError> {", "    new",
                     "++ looks like a header", "}"]
        self.assertEqual(alias.realign_diff(diff, {path: rewritten}), diff.replace(
            " fn f() -> StorageResult<u8> {", " fn f() -> ::core::result::Result<u8, crate::error::StorageError> {"))
        self.assertEqual(alias.realign_diff(diff, {}), diff)

    def test_two_files_several_hunks_are_counted_from_their_headers(self):
        # a.rs: a one-line hunk (`@@ -1 +1 @@`, counts left out), then a hunk with
        # context, removed and added lines; gone.rs: deleted; b.rs: after them, so
        # a hunk counted wrongly in a.rs leaves b.rs's lines as they were.
        a, b = "src/a.rs", "src/b.rs"
        sig = "fn f() -> StorageResult<u8> {"
        new_sig = "fn f() -> ::core::result::Result<u8, crate::error::StorageError> {"
        diff = (f"diff --git a/{a} b/{a}\n--- a/{a}\n+++ b/{a}\n"
                f"@@ -1 +1 @@\n-fn f() -> u8 {{\n+{sig}\n"
                f"@@ -5,4 +5,4 @@ fn f\n ctx5\n-old6\n-old7\n+{sig}\n+new7\n ctx8\n"
                f"diff --git a/src/gone.rs b/src/gone.rs\n--- a/src/gone.rs\n+++ /dev/null\n"
                f"@@ -1,2 +0,0 @@\n-{sig}\n-}}\n"
                f"diff --git a/{b} b/{b}\n--- a/{b}\n+++ b/{b}\n"
                f"@@ -2,3 +2,3 @@\n ctx2\n-x\n+{sig}\n {sig}\n\\ No newline at end of file\n")
        files = {a: [new_sig, "2", "3", "4", "ctx5", new_sig, "new7", "ctx8"],
                 b: ["1", "ctx2", new_sig, new_sig]}
        want = (diff.replace(f"+{sig}\n+new7", f"+{new_sig}\n+new7")
                .replace(f"@@ -1 +1 @@\n-fn f() -> u8 {{\n+{sig}", f"@@ -1 +1 @@\n-fn f() -> u8 {{\n+{new_sig}")
                .replace(f" ctx2\n-x\n+{sig}\n {sig}", f" ctx2\n-x\n+{new_sig}\n {new_sig}"))
        self.assertEqual(want.count(new_sig), 4)
        self.assertEqual(alias.realign_diff(diff, files), want)

    def test_a_diff_whose_new_side_is_not_the_tree_is_refused(self):
        # An in_diff range that does not end at the checked-out head: its line
        # numbers point at other code, so realigning it would hide the mismatch.
        diff = "--- a/x.rs\n+++ b/x.rs\n@@ -4,2 +4,2 @@\n-    2\n+    2 + 0\n }\n"
        tree = ["fn f() -> StorageResult<u8> {", "    Ok(1)", "}", "    0", "}", ""]
        rewritten = [tree[0].replace("StorageResult<u8>", "::core::result::Result<u8, E>")] + tree[1:]
        with self.assertRaises(alias.StaleDiff):
            alias.realign_diff(diff, {"x.rs": rewritten}, {"x.rs": tree})
        matching = "--- a/x.rs\n+++ b/x.rs\n@@ -1,2 +1,2 @@\n fn f() -> StorageResult<u8> {\n-    Ok(0)\n+    Ok(1)\n"
        self.assertIn(" fn f() -> ::core::result::Result<u8, E> {",
                      alias.realign_diff(matching, {"x.rs": rewritten}, {"x.rs": tree}))

    def test_a_form_feed_or_a_crlf_line_survives_the_realignment(self):
        diff = "--- a/x.rs\n+++ b/x.rs\n@@ -1,3 +1,3 @@\n fn f() -> StorageResult<u8> {\r\n-    0 // a\x0cb\r\n+    1 // a\x0cb\r\n }\r\n"
        new, _ = alias.rewrite_source("fn f() -> StorageResult<u8> {\r\n    1 // a\x0cb\r\n}\r\n", STORAGE)
        self.assertEqual(alias.realign_diff(diff, {"x.rs": new.split("\n")}), diff.replace(
            " fn f() -> StorageResult<u8> {", " fn f() -> ::core::result::Result<u8, crate::error::StorageError> {"))

    def test_main_rewrites_the_tree_and_the_diff_or_nothing(self):
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        src = tmp / "crates/persistence/src/backends/cassandra"
        src.mkdir(parents=True)
        (src / "a.rs").write_text("fn f() -> StorageResult<u8> {\n    Ok(1)\n}\n")
        (src / "b.rs").write_text("fn g() -> u8 {\n    1\n}\n")
        (src / "d.rs").write_bytes(b"fn d() -> StorageResult<()> {\r\n    Ok(())\r\n}\r\n")
        diff = tmp / "pr.diff"
        rel = "crates/persistence/src/backends/cassandra/a.rs"
        diff.write_text(f"--- a/{rel}\n+++ b/{rel}\n@@ -1,3 +1,3 @@\n fn f() -> StorageResult<u8> {{\n-    Ok(0)\n+    Ok(1)\n }}\n")
        summary = tmp / "alias.json"
        cwd = Path.cwd()
        try:
            import os
            os.chdir(tmp)
            with redirect_stdout(io.StringIO()):
                status = alias.main(["--alias", "StorageResult=crate::error::StorageError", "--diff", str(diff),
                                     "--summary", str(summary), "crates/persistence/src/backends/cassandra"])
            self.assertEqual(status, 0)
            self.assertIn("-> ::core::result::Result<u8, crate::error::StorageError> {", (src / "a.rs").read_text())
            self.assertIn(" fn f() -> ::core::result::Result<u8, crate::error::StorageError> {\n", diff.read_text())
            self.assertIn("-    Ok(0)\n", diff.read_text())
            # Every rewritten file is written, each with its own newlines.
            self.assertEqual((src / "d.rs").read_bytes(),
                             b"fn d() -> ::core::result::Result<(), crate::error::StorageError> {\r\n    Ok(())\r\n}\r\n")
            self.assertEqual(json.loads(summary.read_text())["files"],
                             {rel: 1, "crates/persistence/src/backends/cassandra/d.rs": 1})
            # A file it cannot rewrite: nothing changes, and it exits 1.
            (src / "a.rs").write_text("fn f() -> StorageResult<u8> {\n")
            (src / "c.rs").write_text("fn h() -> StorageResult<u8 {\n")
            before = diff.read_text()
            with redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(alias.main(["--alias", "StorageResult=crate::error::StorageError", "--diff",
                                             str(diff), "crates/persistence/src/backends/cassandra"]), 1)
            self.assertEqual((src / "a.rs").read_text(), "fn f() -> StorageResult<u8> {\n")
            self.assertEqual(diff.read_text(), before)
            # A diff that does not match the tree: nothing is written, exit 3.
            (src / "c.rs").unlink()
            a_before = (src / "a.rs").read_text()
            diff.write_text(f"--- a/{rel}\n+++ b/{rel}\n@@ -1,1 +1,1 @@\n-fn f() -> u8 {{\n+fn f() -> StorageResult<u16> {{\n")
            stale = diff.read_text()
            with redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(alias.main(["--alias", "StorageResult=crate::error::StorageError", "--diff",
                                             str(diff), "crates/persistence/src/backends/cassandra"]), 3)
            self.assertEqual((src / "a.rs").read_text(), a_before)
            self.assertEqual(diff.read_text(), stale)
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(alias.main(["--alias", "bad", "x"]), 2)
        finally:
            os.chdir(cwd)


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
