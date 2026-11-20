#!/usr/bin/env python3
"""Tests for flaky-tests.py, the `flaky` job of
.github/workflows/cassandra-flaky.yml: which tests a diff adds or changes,
how a run's output is read, and the report. Standard library only:

    PYTHONPATH=.github/scripts python3 -m unittest -v test_flaky_tests

The diffs are `git diff -U0` and `-U3` output for the small files below; the
libtest output is what a test binary prints with `--exact … --show-output`.
"""

from __future__ import annotations

import importlib.util
import io
import json
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("flaky_tests", HERE / "flaky-tests.py")
flaky = importlib.util.module_from_spec(spec)
spec.loader.exec_module(flaky)

LIB = "crates/persistence/src/backends/cassandra/batch.rs"

SOURCE = textwrap.dedent('''\
    pub fn pack(x: &str) -> usize {
        let open = '{';
        let s = "fn not_a_test() { #[test] }";
        let r = r#"#[tokio::test] async fn quoted() {}"#;
        x.len()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn plain() {
            assert_eq!(pack("a"), 1);
        }

        #[test]
        #[ignore = "needs a node"]
        fn ignored_one() {
            let brace = "}";
            assert!(brace.len() == 1);
        }

        #[ignore]
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn flavoured<'a>() {
            /* a { comment */
            let c = '}';
            let _ = c;
        }

        mod inner {
            #[tokio::test]
            async fn nested() {}
        }

        fn helper() {}
    }
    ''')


def write_tree(root: Path, files: dict[str, str]) -> None:
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)


def temp_dir(test: unittest.TestCase) -> Path:
    """A temporary directory that is removed when `test` ends, pass or fail.
    The fmt job runs these tests on every push, on persistent runners."""
    tmp = Path(tempfile.mkdtemp())
    test.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
    return tmp


def git_diff(test: unittest.TestCase, old: dict[str, str], new: dict[str, str],
             context: int = 0) -> tuple[str, Path]:
    """A real `git diff -U<context>` between two trees, and the new tree's
    root, in a directory removed when `test` ends."""
    tmp = temp_dir(test)
    subprocess.run(["git", "init", "-q", str(tmp)], check=True)
    run = lambda *a: subprocess.run(["git", "-C", str(tmp), "-c", "user.name=t", "-c", "user.email=t@t", *a],
                                    check=True, capture_output=True, text=True).stdout
    write_tree(tmp, old)
    run("add", "-A")
    run("commit", "-q", "--allow-empty", "-m", "old")
    for name in old:
        if name not in new:
            (tmp / name).unlink()
    write_tree(tmp, new)
    run("add", "-A")
    run("commit", "-q", "-m", "new")
    return run("diff", f"-U{context}", "--no-color", "HEAD~1", "HEAD"), tmp


class Blank(unittest.TestCase):
    def test_strings_chars_and_comments_are_blanked_with_lines_kept(self):
        code = flaky.blank(SOURCE)
        self.assertEqual(len(code), len(SOURCE))
        self.assertEqual(code.count("\n"), SOURCE.count("\n"))
        self.assertNotIn("not_a_test", code)
        self.assertNotIn("quoted", code)
        self.assertNotIn("a { comment", code)
        # '{' and '}' as chars and "}" as a string leave the braces balanced.
        self.assertEqual(code.count("{"), code.count("}"))
        # A lifetime is not a char literal.
        self.assertIn("flavoured<'a>", code)

    def test_byte_and_raw_byte_strings(self):
        code = flaky.blank('let a = b"{"; let b = br##"}"#"##; let c = rb;')
        self.assertNotIn("{", code)
        self.assertNotIn("}", code)
        self.assertIn("let c = rb;", code)


class TestsIn(unittest.TestCase):
    def test_finds_every_test_attribute_form_and_its_modules(self):
        found = {t["fn"]: t for t in flaky.tests_in(SOURCE)}
        self.assertEqual(sorted(found), ["flavoured", "ignored_one", "nested", "plain"])
        self.assertEqual(found["plain"]["modules"], ["tests"])
        self.assertEqual(found["nested"]["modules"], ["tests", "inner"])
        # The quoted tests in pack's strings are not tests, and helper is not one.
        self.assertNotIn("helper", found)

    def test_span_runs_from_the_first_attribute_to_the_closing_brace(self):
        lines = SOURCE.splitlines()
        found = {t["fn"]: t for t in flaky.tests_in(SOURCE)}
        f = found["flavoured"]
        self.assertEqual(lines[f["start"] - 1].strip(), "#[ignore]")
        self.assertEqual(lines[f["end"] - 1].strip(), "}")
        self.assertTrue(lines[f["fn_line"] - 1].strip().startswith("async fn flavoured"))
        i = found["ignored_one"]
        self.assertEqual(lines[i["start"] - 1].strip(), "#[test]")
        # The "}" string inside the body does not end it early.
        self.assertIn("assert!(brace.len() == 1);", lines[i["end"] - 2])

    def test_cfg_test_alone_is_not_a_test(self):
        self.assertEqual(flaky.tests_in("#[cfg(test)]\nfn not_one() {}\n"), [])

    def test_attribute_on_the_line_above_with_a_doc_comment_between(self):
        src = "#[tokio::test]\n/// doc\n#[allow(unused)]\nasync fn t() {\n}\n"
        found = flaky.tests_in(src)
        self.assertEqual([(t["fn"], t["start"], t["end"]) for t in found], [("t", 1, 5)])


class ChangedLines(unittest.TestCase):
    def test_added_lines_and_a_removal_point(self):
        diff = textwrap.dedent('''\
            diff --git a/x.rs b/x.rs
            --- a/x.rs
            +++ b/x.rs
            @@ -3,0 +4,2 @@ fn a() {
            +    one();
            +    two();
            @@ -10,2 +11,0 @@ fn b() {
            -    gone();
            -    gone_too();
            ''')
        change = flaky.changed_lines(diff)["x.rs"]
        self.assertEqual(change["added"], {4, 5})
        self.assertEqual(change["removed_at"], {11})

    def test_a_new_file_and_a_deleted_one(self):
        diff, _ = git_diff(self, {"gone.rs": "fn x() {}\n"}, {"new.rs": "fn y() {}\n"})
        change = flaky.changed_lines(diff)
        self.assertEqual(set(change), {"new.rs"})
        self.assertEqual(change["new.rs"]["added"], {1})


class Find(unittest.TestCase):
    def setUp(self):
        self.old = {LIB: SOURCE}

    def changed(self, new_source: str, context: int = 0, extra_old=None, extra_new=None) -> dict:
        old = dict(self.old, **(extra_old or {}))
        new = {LIB: new_source, **(extra_new or {})}
        diff, root = git_diff(self, old, new, context)
        return {t["name"]: t for t in flaky.find(diff, root)["tests"]}

    def test_a_changed_body_counts(self):
        found = self.changed(SOURCE.replace('assert_eq!(pack("a"), 1);', 'assert_eq!(pack("ab"), 2);'))
        self.assertEqual(list(found), ["backends::cassandra::batch::tests::plain"])
        self.assertEqual(found["backends::cassandra::batch::tests::plain"]["change"], "changed")

    def test_an_attribute_on_the_line_above_counts(self):
        found = self.changed(SOURCE.replace("    #[ignore]\n    #[tokio::test(", "    #[tokio::test(", 1))
        self.assertEqual(list(found), ["backends::cassandra::batch::tests::flavoured"])

    def test_a_rename_is_the_new_name(self):
        found = self.changed(SOURCE.replace("fn plain()", "fn plain_renamed()"))
        self.assertEqual(list(found), ["backends::cassandra::batch::tests::plain_renamed"])

    def test_a_removed_line_inside_a_test_counts(self):
        found = self.changed(SOURCE.replace('        let brace = "}";\n', ""), context=3)
        self.assertEqual(list(found), ["backends::cassandra::batch::tests::ignored_one"])

    def test_an_added_test_is_added_and_nested_modules_are_named(self):
        new = SOURCE.replace("    fn helper() {}\n",
                             "    fn helper() {}\n\n    #[test]\n    fn brand_new() {\n        helper();\n    }\n")
        found = self.changed(new)
        self.assertEqual(found["backends::cassandra::batch::tests::brand_new"]["change"], "added")
        self.assertEqual(len(found), 1)

    def test_a_change_outside_any_test_finds_nothing(self):
        self.assertEqual(self.changed(SOURCE.replace("x.len()", "x.len() + 0")), {})

    def test_quoted_tests_in_strings_are_never_found(self):
        self.assertEqual(self.changed(SOURCE.replace("fn not_a_test()", "fn still_not_a_test()")), {})

    def test_integration_tests_take_the_module_their_crate_gives_them(self):
        root_rs = '#[path = "cassandra/create_read.rs"]\nmod create_read;\n// mod commented_out;\nmod common {\n    #[path = "../common/x.rs"]\n    pub mod x;\n}\n'
        old_t = "#[tokio::test]\nasync fn reads() {\n    one();\n}\n"
        new_t = "#[tokio::test]\nasync fn reads() {\n    one();\n    two();\n}\n"
        found = self.changed(SOURCE,
                             extra_old={"crates/persistence/tests/cassandra_tests.rs": root_rs,
                                        "crates/persistence/tests/cassandra/create_read.rs": old_t},
                             extra_new={"crates/persistence/tests/cassandra_tests.rs": root_rs,
                                        "crates/persistence/tests/cassandra/create_read.rs": new_t})
        self.assertEqual(list(found), ["create_read::reads"])
        self.assertEqual(found["create_read::reads"]["target"], "cassandra_tests")

    def test_a_file_no_test_crate_includes_is_a_warning(self):
        diff, root = git_diff(self, {LIB: SOURCE}, {LIB: SOURCE, "crates/persistence/tests/cassandra/orphan.rs": "#[test]\nfn t() {}\n"})
        result = flaky.find(diff, root)
        self.assertEqual(result["tests"], [])
        self.assertTrue(any("orphan.rs" in w for w in result["warnings"]))

    def test_files_outside_the_scope_are_ignored(self):
        diff, root = git_diff(self, {"crates/persistence/src/backends/sqlite/x.rs": "fn a() {}\n"},
                              {"crates/persistence/src/backends/sqlite/x.rs": "#[test]\nfn a() {}\n"})
        self.assertEqual(flaky.find(diff, root)["tests"], [])


class Bins(unittest.TestCase):
    def test_lib_and_test_executables(self):
        lines = [
            {"reason": "compiler-artifact", "target": {"kind": ["lib"], "name": "helios_persistence"},
             "profile": {"test": False}, "executable": None},
            {"reason": "compiler-artifact", "target": {"kind": ["lib"], "name": "helios_persistence"},
             "profile": {"test": True}, "executable": "/t/deps/helios_persistence-1"},
            {"reason": "compiler-artifact", "target": {"kind": ["test"], "name": "cassandra_tests"},
             "profile": {"test": True}, "executable": "/t/deps/cassandra_tests-2"},
            {"reason": "build-finished", "success": True},
        ]
        text = "\n".join(json.dumps(x) for x in lines) + "\nnot json\n"
        self.assertEqual(flaky.bins(text), {"lib": "/t/deps/helios_persistence-1", "cassandra_tests": "/t/deps/cassandra_tests-2"})


PASS = "\nrunning 1 test\ntest backends::cassandra::batch::tests::plain ... ok\n\nsuccesses:\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 185 filtered out; finished in 0.01s\n\n"
SKIP = PASS.replace("successes:\n", "successes:\n\n---- t stdout ----\nskipping: HFS_TEST_CASSANDRA_URL is not set\n")
FAIL = textwrap.dedent('''\

    running 1 test
    test backends::cassandra::batch::tests::plain ... FAILED

    failures:

    ---- backends::cassandra::batch::tests::plain stdout ----

    thread 'backends::cassandra::batch::tests::plain' panicked at crates/persistence/src/backends/cassandra/batch.rs:14:9:
    assertion `left == right` failed
      left: 2
     right: 1
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


    failures:
        backends::cassandra::batch::tests::plain

    test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 185 filtered out; finished in 0.01s

    ''')
NONE = "\nrunning 0 tests\n\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 186 filtered out; finished in 0.00s\n\n"
IGNORED = "\nrunning 1 test\ntest t ... ignored, needs a node\n\ntest result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 185 filtered out; finished in 0.00s\n\n"


class Classify(unittest.TestCase):
    def test_outcomes(self):
        self.assertEqual(flaky.classify(PASS, 0, False)["outcome"], "pass")
        self.assertEqual(flaky.classify(FAIL, 101, False)["outcome"], "fail")
        self.assertEqual(flaky.classify(NONE, 0, False)["outcome"], "not run")
        self.assertEqual(flaky.classify(IGNORED, 0, False)["outcome"], "ignored")
        self.assertEqual(flaky.classify("", 124, True)["outcome"], "timeout")
        self.assertEqual(flaky.classify("Segmentation fault", 139, False)["outcome"], "error")
        self.assertTrue(flaky.classify(SKIP, 0, False)["skipped"])
        self.assertFalse(flaky.classify(PASS, 0, False)["skipped"])
        # ANSI colour codes around the result line.
        self.assertEqual(flaky.classify("\x1b[32m" + PASS + "\x1b[0m", 0, False)["outcome"], "pass")

    def test_failure_message_is_the_panic_and_its_message(self):
        msg = flaky.failure_message(FAIL)
        self.assertIn("panicked at crates/persistence/src/backends/cassandra/batch.rs:14:9:", msg)
        self.assertIn("left: 2", msg)
        self.assertNotIn("RUST_BACKTRACE", msg)


class Run(unittest.TestCase):
    def test_run_counts_each_attempt_with_a_fake_binary(self):
        tmp = temp_dir(self)
        counter = tmp / "n"
        counter.write_text("0")
        (tmp / "pass.txt").write_text(PASS)
        (tmp / "fail.txt").write_text(FAIL)
        fake = tmp / "fake-test"
        # Fails every third run, as a flaky test might.
        fake.write_text(f"#!/bin/sh\nn=$(($(cat {counter}) + 1)); echo $n > {counter}\n"
                        f"if [ $((n % 3)) -eq 0 ]; then cat {tmp}/fail.txt; exit 101; fi\ncat {tmp}/pass.txt\n")
        fake.chmod(0o755)
        tests = [{"target": "lib", "name": "backends::cassandra::batch::tests::plain", "file": LIB, "line": 13, "change": "changed"},
                 {"target": "cassandra_tests", "name": "x::y", "file": "f", "line": 1, "change": "added"}]
        with redirect_stdout(io.StringIO()):
            result = flaky.run(tests, {"lib": str(fake)}, 6, tmp / "logs", 30)
        runs = result["results"][0]["runs"]
        self.assertEqual([r["outcome"] for r in runs], ["pass", "pass", "fail", "pass", "pass", "fail"])
        self.assertIn("no test binary", result["results"][1]["error"])
        md, data = flaky.report(result)
        row = data["tests"][0]
        self.assertTrue(row["flaky"])
        self.assertEqual((row["runs"], row["passes"], row["failures"]), (6, 4, 2))
        self.assertTrue(any("Flaky: backends::cassandra::batch::tests::plain failed 2 of 6 runs" in w for w in data["warnings"]))
        self.assertIn("| `backends::cassandra::batch::tests::plain` | lib | changed | 6 | 4 | 2 | 0 | flaky |", md)
        self.assertTrue((tmp / "logs" / result["results"][0]["log"]).exists())


class Budget(unittest.TestCase):
    def test_runs_past_the_budget_are_not_reached(self):
        tmp = temp_dir(self)
        (tmp / "pass.txt").write_text(PASS)
        fake = tmp / "fake-test"
        fake.write_text(f"#!/bin/sh\ncat {tmp}/pass.txt\n")
        fake.chmod(0o755)
        tests = [{"target": "lib", "name": "a", "file": LIB, "line": 1, "change": "added"},
                 {"target": "lib", "name": "b", "file": LIB, "line": 2, "change": "added"}]
        with redirect_stdout(io.StringIO()):
            result = flaky.run(tests, {"lib": str(fake)}, 3, tmp / "logs", 30, budget_s=0)
        self.assertTrue(result["stopped_at_budget"])
        self.assertEqual([r["not_reached"] for r in result["results"]], [3, 3])
        _, data = flaky.report(result)
        self.assertTrue(any("3 of its runs were not reached" in w for w in data["warnings"]))


class Report(unittest.TestCase):
    def test_no_tests_says_so(self):
        md, data = flaky.report({"runs_per_test": 20, "results": []})
        self.assertIn("No test added or changed", md)
        self.assertEqual(data["tests"], [])

    def test_tests_found_but_none_run_is_a_warning(self):
        md, data = flaky.report({"results": []}, {"tests": [{"name": "a"}], "warnings": []})
        self.assertIn("1 test(s) were found in the diff, but none was run", md)
        self.assertIn("Flaky tests: 1 test(s) found, none run", data["warnings"])

    def test_stable_skipped_and_not_run(self):
        ok = {"outcome": "pass", "seconds": 0.1, "skipped": False}
        skip = {"outcome": "pass", "seconds": 0.1, "skipped": True}
        none = {"outcome": "not run", "seconds": 0.1, "skipped": False, "message": "x"}
        results = {"runs_per_test": 2, "timeout_s": 300, "results": [
            {"target": "lib", "name": "a", "runs": [ok, ok]},
            {"target": "lib", "name": "b", "runs": [skip, skip]},
            {"target": "lib", "name": "c", "runs": [none, none]},
        ]}
        md, data = flaky.report(results)
        status = {r[0]: r for r in (line.split(" | ") for line in md.splitlines() if line.startswith("| `"))}
        self.assertIn("stable", status["| `a`"])
        self.assertIn("skipped (no node?)", status["| `b`"])
        self.assertTrue(any("c: matched no test" in w for w in data["warnings"]))
        self.assertIn("not run (its name matched no test)", status["| `c`"])
        self.assertFalse(any("c failed every one" in w for w in data["warnings"]))


class Main(unittest.TestCase):
    def test_report_with_no_results_file_exits_0(self):
        tmp = temp_dir(self)
        with redirect_stdout(io.StringIO()):
            self.assertEqual(flaky.main(["report", "--results", str(tmp / "missing.json"), "--out", str(tmp / "out")]), 0)
        self.assertIn("No test added or changed", (tmp / "out" / "flaky.md").read_text())

    def test_find_writes_its_json(self):
        diff, root = git_diff(self, {LIB: SOURCE}, {LIB: SOURCE.replace("fn plain()", "fn plain2()")})
        (root / "d.diff").write_text(diff)
        with redirect_stdout(io.StringIO()):
            flaky.main(["find", "--diff", str(root / "d.diff"), "--root", str(root), "--out", str(root / "t.json")])
        self.assertEqual([t["name"] for t in json.loads((root / "t.json").read_text())["tests"]],
                         ["backends::cassandra::batch::tests::plain2"])



class NoTempDirLeft(unittest.TestCase):
    """The fmt job runs this module on every push, on persistent runners, so
    every other test here must leave its temporary directory removed."""

    def test_the_other_tests_leave_no_temporary_directory(self):
        scratch = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, scratch, ignore_errors=True)
        loader = unittest.TestLoader()
        suite = unittest.TestSuite(
            loader.loadTestsFromTestCase(case)
            for case in (Blank, TestsIn, ChangedLines, Find, Bins, Classify, Run, Budget, Report, Main))
        saved = tempfile.tempdir
        tempfile.tempdir = str(scratch)
        try:
            result = unittest.TestResult()
            suite.run(result)
        finally:
            tempfile.tempdir = saved
        self.assertTrue(result.wasSuccessful(), result.errors + result.failures)
        self.assertEqual(sorted(p.name for p in scratch.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
