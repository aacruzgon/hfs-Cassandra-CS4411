#!/usr/bin/env python3
"""Tests for cassandra-coverage-rest-summary.py, the coverage report's
crates/rest section. Standard library only:

    python3 -m unittest discover -s .github/scripts -p 'test_cassandra_coverage_rest_summary.py'

The exports are small hand-written `cargo llvm-cov report --json` documents,
with the fields the script reads: data[].files[].filename/summary and
data[].functions[].name/count/filenames/regions (a region is
[line_start, col_start, line_end, col_end, count, file_id, expanded_file_id,
kind], kind 0 for code).
"""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("rest_summary", HERE / "cassandra-coverage-rest-summary.py")
rest = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rest)

BATCH_RS = """\
use x;

pub(crate) async fn check_entry_if_match<S>(state: S) -> bool {
    let f = |x| x + 1;
    f(1) > 0
}

fn process_batch_entry() {}
"""

PATCH_RS = """\
/// fn not_an_item() in a comment
pub async fn patch_handler<S>() {
    async move {}.await
}
"""


def summary(lines, regions, functions):
    return {"lines": {"covered": lines[0], "count": lines[1]},
            "regions": {"covered": regions[0], "count": regions[1]},
            "functions": {"covered": functions[0], "count": functions[1]}}


def function(name, file, line, count):
    return {"name": name, "count": count, "filenames": [f"/w/crates/rest/src/{file}"],
            "regions": [[line, 1, line + 1, 2, count, 0, 0, 0]]}


def export(files, functions):
    return {"data": [{"files": [{"filename": f"/w/crates/rest/src/{n}", "summary": s} for n, s in files.items()]
                      + [{"filename": "/w/crates/persistence/src/backends/cassandra/read.rs", "summary": summary((9, 9), (9, 9), (9, 9))}],
                      "functions": functions}]}


class Fixture(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        root = Path(self.dir.name)
        (root / "crates/rest/src/handlers").mkdir(parents=True)
        (root / "crates/rest/src/handlers/batch.rs").write_text(BATCH_RS)
        (root / "crates/rest/src/handlers/patch.rs").write_text(PATCH_RS)
        self.root = root
        self.sources = rest.Sources(str(root))

    def tearDown(self):
        self.dir.cleanup()

    def write(self, name, document):
        path = self.root / name
        path.write_text(json.dumps(document))
        return str(path)


class Naming(Fixture):
    def test_repo_path_keeps_only_crates_rest_src(self):
        self.assertEqual(rest.repo_path("/a/b/crates/rest/src/handlers/batch.rs"), "handlers/batch.rs")
        self.assertEqual(rest.repo_path("C:\\w\\crates\\rest\\src\\error.rs"), "error.rs")
        self.assertIsNone(rest.repo_path("/a/crates/rest/tests/x.rs"))
        self.assertIsNone(rest.repo_path("/a/crates/persistence/src/lib.rs"))

    def test_a_closure_folds_into_its_fn(self):
        self.assertEqual(self.sources.enclosing("handlers/batch.rs", 3), "check_entry_if_match")
        self.assertEqual(self.sources.enclosing("handlers/batch.rs", 4), "check_entry_if_match")
        self.assertEqual(self.sources.enclosing("handlers/batch.rs", 8), "process_batch_entry")

    def test_a_comment_is_not_an_item_and_nothing_above_the_first_is_named(self):
        self.assertIsNone(self.sources.enclosing("handlers/patch.rs", 1))
        self.assertEqual(self.sources.enclosing("handlers/patch.rs", 3), "patch_handler")

    def test_a_missing_source_file_names_nothing(self):
        self.assertIsNone(self.sources.enclosing("handlers/gone.rs", 10))


class Summary(Fixture):
    def exports(self):
        files_all = {"handlers/batch.rs": summary((6, 8), (5, 9), (2, 2)),
                     "handlers/patch.rs": summary((3, 3), (2, 2), (1, 1)),
                     "error.rs": summary((1, 10), (1, 12), (1, 4))}
        files_without = {"handlers/batch.rs": summary((1, 8), (1, 9), (1, 2)),
                         "handlers/patch.rs": summary((0, 3), (0, 2), (0, 1)),
                         "error.rs": summary((1, 10), (1, 12), (1, 4))}
        fns_all = [
            function("_RNvCs1_11helios_rest5check", "handlers/batch.rs", 3, 4),   # the fn
            function("_RNCNvCs1_11helios_rest5check0", "handlers/batch.rs", 4, 4),  # its closure
            function("_RINvCs1_11helios_rest5checkB_E", "handlers/batch.rs", 3, 1),  # another instance
            function("_RNvCs1_11helios_rest7process", "handlers/batch.rs", 8, 2),
            function("_RNvCs1_11helios_rest5patch", "handlers/patch.rs", 2, 3),
        ]
        fns_without = [
            function("_RNvCs1_11helios_rest5check", "handlers/batch.rs", 3, 0),
            function("_RNCNvCs1_11helios_rest5check0", "handlers/batch.rs", 4, 0),
            function("_RINvCs1_11helios_rest5checkB_E", "handlers/batch.rs", 3, 0),
            function("_RNvCs1_11helios_rest7process", "handlers/batch.rs", 8, 1),
            function("_RNvCs1_11helios_rest5patch", "handlers/patch.rs", 2, 0),
        ]
        return (self.write("all.json", export(files_all, fns_all)),
                self.write("without.json", export(files_without, fns_without)))

    def test_instances_and_closures_count_as_one_function_with_summed_calls(self):
        all_path, _ = self.exports()
        loaded = rest.load(all_path, self.sources)
        self.assertEqual(loaded["functions"][("handlers/batch.rs", 3)], 9)
        self.assertEqual(rest.calls_named(loaded, "handlers/batch.rs", "check_entry_if_match"), 9)
        self.assertEqual(rest.totals(loaded)["functions"], {"covered": 3, "count": 3, "percent": 100.0})

    def test_totals_keep_llvm_cov_lines_and_regions_and_only_rest_files(self):
        all_path, _ = self.exports()
        t = rest.totals(rest.load(all_path, self.sources))
        self.assertEqual((t["lines"]["covered"], t["lines"]["count"]), (10, 21))
        self.assertEqual((t["regions"]["covered"], t["regions"]["count"]), (8, 23))

    def test_step3_difference(self):
        all_path, without_path = self.exports()
        report = rest.summarise(rest.load(all_path, self.sources), rest.load(without_path, self.sources))
        self.assertEqual(report["without_step3"]["lines"]["covered"], 2)
        self.assertEqual([c["file"] for c in report["files_step3_changes"]], ["handlers/batch.rs", "handlers/patch.rs"])
        self.assertEqual(report["only_step3"]["count"], 2)
        self.assertEqual({f["function"] for f in report["only_step3"]["functions"]}, {"check_entry_if_match", "patch_handler"})
        watch = {w["function"]: w for w in report["watch"]}
        self.assertEqual((watch["check_entry_if_match"]["calls"], watch["check_entry_if_match"]["calls_without_step3"]), (9, 0))
        self.assertEqual((watch["process_batch_entry"]["calls"], watch["process_batch_entry"]["calls_without_step3"]), (2, 1))
        self.assertFalse(watch["delete_handler"]["present"])
        self.assertIsNone(watch["delete_handler"]["calls"])

    def test_markdown_without_the_second_export_says_so(self):
        all_path, _ = self.exports()
        report = rest.summarise(rest.load(all_path, self.sources), None)
        text = rest.markdown(report, {"cargo_llvm_cov": "cargo-llvm-cov 0.9.1"})
        self.assertIn("not available", text)
        self.assertIn("| PATCH | `handlers/patch.rs`: `patch_handler` | 3 | – |", text)
        self.assertIn("| DELETE | `handlers/delete.rs`: `delete_handler` | not in the export | – |", text)
        self.assertNotIn("Functions only step 3 reaches", text)

    def test_main_writes_both_files_and_tolerates_an_unreadable_second_export(self):
        all_path, _ = self.exports()
        md, js = self.root / "s.md", self.root / "s.json"
        rest.main(["x", all_path, str(self.root / "missing.json"), str(md), str(js), str(self.root)])
        data = json.loads(js.read_text())
        self.assertIsNone(data["without_step3"])
        self.assertEqual(data["total"]["lines"]["covered"], 10)
        self.assertIn("| Every test |", md.read_text())

    def test_main_with_both_exports(self):
        all_path, without_path = self.exports()
        md, js = self.root / "s.md", self.root / "s.json"
        rest.main(["x", all_path, without_path, str(md), str(js), str(self.root)])
        text = md.read_text()
        self.assertIn("| Batch entry `ifMatch` check | `handlers/batch.rs`: `check_entry_if_match` | 9 | 0 |", text)
        self.assertIn("Functions only step 3 reaches: **2**", text)
        self.assertIn("| `handlers/batch.rs` | 1 | 6 | 8 |", text)


ERROR_RS = """\
pub struct A; pub struct B; pub struct E;
impl From<A> for E {
    fn from(_: A) -> E {
        E
    }
}
impl From<B> for E {
    fn from(_: B) -> E {
        E
    }
}
fn helper() {}
static TABLE: std::sync::LazyLock<u32> = std::sync::LazyLock::new(|| {
    7
});
"""


STATE_RS = """\
impl State {
    pub fn new<T>(t: T) -> Self
    where
        T: Into<u32>,
    {
        let n = t.into();
        let s = Self { n };
        s
    }

    pub fn get(&self) -> u32 { self.n }
    fn declared(&self) -> u32;
}
static S: u32 = 1;
"""


class FnItems(Fixture):
    """Functions are counted per `fn` item: two items with one name stay two,
    and code outside every item (a static's closure) is no function."""

    def setUp(self):
        super().setUp()
        (self.root / "crates/rest/src/error.rs").write_text(ERROR_RS)

    def error_export(self, from_a, from_b, helper, table):
        fns = [function("_RNvXs_from_A", "error.rs", 4, from_a),
               function("_RNvXs0_from_B", "error.rs", 9, from_b),
               function("_RNvhelper", "error.rs", 12, helper),
               function("_RNCNvTABLE0", "error.rs", 13, table)]
        return export({"error.rs": summary((3, 6), (3, 6), (2, 4))}, fns)

    def test_one_of_three_items_covered_is_one_third(self):
        # from<A> ran, from<B> and helper never did; the static's closure ran.
        path = self.write("error.json", self.error_export(5, 0, 0, 1))
        self.assertEqual(rest.totals(rest.load(path, self.sources))["functions"],
                         {"covered": 1, "count": 3, "percent": 33.33})

    def test_an_item_spans_to_its_closing_brace(self):
        self.assertEqual(self.sources.item("error.rs", 3), (3, "from"))
        self.assertEqual(self.sources.item("error.rs", 5), (3, "from"))
        self.assertEqual(self.sources.item("error.rs", 8), (8, "from"))
        self.assertEqual(self.sources.item("error.rs", 12), (12, "helper"))
        self.assertIsNone(self.sources.item("error.rs", 13))
        self.assertIsNone(self.sources.item("error.rs", 6))
        self.assertEqual(self.sources.item("handlers/batch.rs", 8), (8, "process_batch_entry"))

    def test_a_struct_literal_inside_a_body_does_not_end_it(self):
        (self.root / "crates/rest/src/state.rs").write_text(STATE_RS)
        self.assertEqual(self.sources.item("state.rs", 7), (2, "new"))
        self.assertEqual(self.sources.item("state.rs", 9), (2, "new"))
        self.assertIsNone(self.sources.item("state.rs", 10))
        self.assertEqual(self.sources.item("state.rs", 11), (11, "get"))
        self.assertEqual(self.sources.item("state.rs", 12), (12, "declared"))
        self.assertIsNone(self.sources.item("state.rs", 14))

    def test_same_named_items_are_listed_apart(self):
        all_path = self.write("all.json", self.error_export(5, 2, 0, 1))
        without_path = self.write("without.json", self.error_export(0, 2, 0, 0))
        report = rest.summarise(rest.load(all_path, self.sources), rest.load(without_path, self.sources))
        self.assertEqual(report["only_step3"]["functions"],
                         [{"file": "error.rs", "function": "from", "line": 3, "calls": 5}])
        self.assertEqual(report["total"]["functions"]["count"], 3)
        text = rest.markdown(report, {})
        self.assertIn("Functions only step 3 reaches: **1**: `error.rs:3`: `from` (5).", text)


if __name__ == "__main__":
    unittest.main()
