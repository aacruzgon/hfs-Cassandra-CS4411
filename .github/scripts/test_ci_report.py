#!/usr/bin/env python3
"""Tests for ci-report.py's Step 3 commands: the CQL audit check (plan D6) and
the report parsers (R2, R4, R6, R7, R10). Standard library only, as in
.github/scripts/fhir-bench/test_*.py:

    python3 -m unittest discover -s .github/scripts -p 'test_ci_report.py'

The audit fixtures are real `auditlogviewer` output, from a Cassandra 5.0.9
node (`cassandra:5.0.9-trixie`, `docker/cassandra`) with
`nodetool enableauditlog --included-keyspaces helios --included-categories
DML,ERROR,PREPARE`, after the statements each fixture names were sent with
cqlsh; FAILED_BATCHES with `--excluded-keyspaces system,…` (as CI runs it)
and the cqlsh's own Python driver sending native-protocol batches.
"""

from __future__ import annotations

import importlib.util
import io
import json
import re
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "ci-report.py"
spec = importlib.util.spec_from_file_location("ci_report", SCRIPT)
ci_report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci_report)

HEAD = "LogMessage: user:anonymous|host:/192.168.148.2:7000|source:/127.0.0.1|port:52822|timestamp:1794020082567"

# Recorded: an X1-style insert and delete on resources_by_type, a CQL-text
# batch holding both, a DELETE on resources, an invalid DELETE on resources
# (REQUEST_FAILURE) and a DELETE on a quoted "resource_versions".
RECORDED = """Type: audit
LogMessage: user:anonymous|host:/192.168.148.2:7000|source:/127.0.0.1|port:52822|timestamp:1794020082567|type:UPDATE|category:DML|ks:helios|scope:resources_by_type|operation:INSERT INTO resources_by_type (tenant_id, resource_type, last_updated, id, version_id) VALUES ('t','Patient','2026-01-01',  'a', 1);
Type: audit
LogMessage: user:anonymous|host:/192.168.148.2:7000|source:/127.0.0.1|port:52822|timestamp:1794020082577|type:DELETE|category:DML|ks:helios|scope:resources_by_type|operation:DELETE FROM resources_by_type WHERE tenant_id='t' AND resource_type='Patient' AND last_updated='2026-01-01' AND id='a';
Type: audit
LogMessage: user:anonymous|host:/192.168.148.2:7000|source:/127.0.0.1|port:52822|timestamp:1794020082584|type:BATCH|category:DML|ks:helios|operation:BEGIN BATCH INSERT INTO resources_by_type (tenant_id, resource_type, last_updated, id, version_id) VALUES ('t','Patient','2026-01-01','b',1); DELETE FROM resources_by_type WHERE tenant_id='t' AND resource_type='Patient' AND last_updated='2026-01-01' AND id='b'; APPLY BATCH;
Type: audit
LogMessage: user:anonymous|host:/192.168.148.2:7000|source:/127.0.0.1|port:52822|timestamp:1794020082595|type:DELETE|category:DML|ks:helios|scope:resources|operation:DELETE FROM resources WHERE tenant_id='t' AND resource_type='Patient' AND id='zz';
Type: audit
LogMessage: user:anonymous|host:/192.168.148.2:7000|source:/127.0.0.1|port:60620|timestamp:1794020100916|type:REQUEST_FAILURE|category:ERROR|ks:helios|operation:DELETE FROM resources WHERE tenant_id='t';; Some partition key parts are missing: resource_type, id
Type: audit
LogMessage: user:anonymous|host:/192.168.148.2:7000|source:/127.0.0.1|port:60620|timestamp:1794020100917|type:DELETE|category:DML|ks:helios|scope:resource_versions|operation:DELETE FROM "resource_versions" WHERE tenant_id='t' AND resource_type='Patient' AND id='q' AND version_id = 1;
"""

# Recorded: failed native-protocol batches. A LOGGED batch of 400 DELETEs on
# resources under a 1 ms write timeout (WriteTimeout; the batchlog applied all
# 400 about a minute later), from a session on keyspace helios; a batch with
# conditions across partitions, from a session with no keyspace, so the entry
# has none; and a failed single statement for comparison.
FAILED_BATCHES = """Type: audit
LogMessage: user:anonymous|host:/192.168.148.2:7000|source:/127.0.0.1|port:44912|timestamp:1794025976515|type:REQUEST_FAILURE|category:ERROR|ks:helios|operation:BATCH of 400 statements at consistency ONE; Operation timed out - received only 0 responses.
Type: audit
LogMessage: user:anonymous|host:/192.168.148.2:7000|source:/127.0.0.1|port:51436|timestamp:1794026122987|type:REQUEST_FAILURE|category:ERROR|operation:BATCH of 2 statements at consistency ONE; Batch with conditions cannot span multiple partitions
Type: audit
LogMessage: user:anonymous|host:/192.168.148.2:7000|source:/127.0.0.1|port:51494|timestamp:1794026123525|type:REQUEST_FAILURE|category:ERROR|operation:INSERT INTO system_auth.nope (a) VALUES (1);; table nope does not exist
"""


def entry(type_, scope, operation, ks="helios", category="DML"):
    scope_part = f"|scope:{scope}" if scope else ""
    return f"Type: audit\n{HEAD}|type:{type_}|category:{category}|ks:{ks}{scope_part}|operation:{operation}\n"


# What Step 3's backend legitimately sends: prepared statements (logged with
# their `?`), the CAS soft delete, I2 and the X1 delete.
CLEAN = "".join([
    entry("UPDATE", "pending_index_writes", "INSERT INTO pending_index_writes (bucket, shard, enqueued_at, tenant_id, resource_type, id, write_id, version_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"),
    entry("UPDATE", "resources", "UPDATE resources SET deleted_at = ?, version_id = ?, last_updated = ? WHERE tenant_id = ? AND resource_type = ? AND id = ? IF version_id = ? AND deleted_at = 0"),
    entry("UPDATE", "resource_versions", "INSERT INTO resource_versions (tenant_id, resource_type, id, version_id, deleted_at) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?"),
    entry("DELETE", "resources_by_type", "DELETE FROM resources_by_type USING TIMESTAMP ? WHERE tenant_id = ? AND resource_type = ? AND last_updated = ? AND id = ?"),
    entry("DELETE", "pending_index_writes", "DELETE FROM pending_index_writes WHERE bucket = ? AND shard = ? AND enqueued_at = ? AND tenant_id = ? AND resource_type = ? AND id = ?"),
    entry("PREPARE_STATEMENT", None, "SELECT version_id FROM resources WHERE tenant_id = ? AND resource_type = ? AND id = ?", category="PREPARE"),
])


class AuditEntries(unittest.TestCase):
    def test_recorded_fields(self):
        entries = ci_report.audit_records(RECORDED)
        self.assertEqual(len(entries), 6)
        self.assertEqual([(e["type"], e["scope"]) for e in entries], [
            ("UPDATE", "resources_by_type"), ("DELETE", "resources_by_type"), ("BATCH", None),
            ("DELETE", "resources"), ("REQUEST_FAILURE", None), ("DELETE", "resource_versions"),
        ])
        self.assertTrue(all(e["ks"] == "helios" for e in entries))
        self.assertTrue(entries[0]["operation"].startswith("INSERT INTO resources_by_type"))

    def test_fields_are_read_before_the_operation_only(self):
        # The CQL text is free text: a pipe-and-colon look-alike inside it is
        # not a field.
        [e] = ci_report.audit_records(entry("UPDATE", "resources_by_type", "INSERT INTO resources_by_type (x) VALUES ('|scope:resources|type:DELETE|')"))
        self.assertEqual((e["type"], e["scope"]), ("UPDATE", "resources_by_type"))
        self.assertIsNone(ci_report.audit_violation(e, "helios"))

    def test_multi_line_operation(self):
        text = entry("DELETE", "resources", "DELETE FROM resources\n  WHERE tenant_id = ?\n  AND id = ?") + entry("UPDATE", "resources", "UPDATE resources SET x = ? WHERE a = ? IF b = ?")
        entries = ci_report.audit_records(text)
        self.assertEqual(len(entries), 2)
        self.assertIn("AND id = ?", entries[0]["operation"])

    def test_recorded_violations(self):
        reasons = [ci_report.audit_violation(e, "helios") for e in ci_report.audit_records(RECORDED)]
        self.assertEqual([r is not None for r in reasons], [False, False, False, True, True, True])
        self.assertIn("scope:resources", reasons[3])
        self.assertIn("REQUEST_FAILURE", reasons[4])
        self.assertIn("scope:resource_versions", reasons[5])

    def test_clean_step3_traffic_has_no_violation(self):
        for e in ci_report.audit_records(CLEAN):
            self.assertIsNone(ci_report.audit_violation(e, "helios"), e)

    def test_delete_text_matches_exactly_the_protected_tables(self):
        hits = [
            "DELETE FROM resources WHERE a = ?",
            "delete from helios.resources where a = ?",
            'DELETE FROM "helios"."resource_versions" WHERE a = ?',
            "DELETE data, deleted_at FROM resources WHERE a = ?",
            "BEGIN BATCH INSERT INTO resources_by_type (a) VALUES (?); DELETE FROM resource_versions WHERE a = ?; APPLY BATCH",
            "DELETE FROM resources",
        ]
        misses = [
            "DELETE FROM resources_by_type WHERE a = ?",
            "DELETE FROM helios.resources_by_type USING TIMESTAMP ? WHERE a = ?",
            "DELETE FROM pending_index_writes WHERE a = ?",
            "DELETE FROM resource_versions_old WHERE a = ?",
            "SELECT deleted_at FROM resources WHERE a = ?",
            "UPDATE resources SET deleted_at = ? WHERE a = ? IF version_id = ?",
            "DELETE FROM resources_by_type WHERE a = ?; SELECT * FROM resources",
        ]
        for text in hits:
            self.assertIsNotNone(ci_report.DELETE_TEXT.search(text), text)
        for text in misses:
            self.assertIsNone(ci_report.DELETE_TEXT.search(text), text)

    def test_native_batch_child_scoped_failure_and_table_wipes(self):
        # A native-protocol BATCH logs each child with its own type and scope,
        # and a |batch:<uuid>| field; a failed prepared statement keeps its
        # scope but becomes REQUEST_FAILURE; TRUNCATE is DDL with a scope
        # (checked on a 5.0.9 node: type:TRUNCATE|category:DDL|ks:helios|scope:resource_versions).
        child = entry("DELETE", "resources", "DELETE FROM resources WHERE tenant_id = ? AND resource_type = ? AND id = ?").replace(
            "|category:DML|", "|category:DML|batch:6a1f6b2e-0000-4000-8000-000000000000|")
        failed = entry("REQUEST_FAILURE", "resources", "DELETE FROM resources WHERE tenant_id = ?; Operation timed out", category="ERROR")
        wipe = entry("TRUNCATE", "resource_versions", "TRUNCATE resource_versions;", category="DDL")
        other_wipe = entry("TRUNCATE", "resources_by_type", "TRUNCATE resources_by_type;", category="DDL")
        entries = ci_report.audit_records(child + failed + wipe + other_wipe)
        self.assertEqual(entries[0]["batch"], "6a1f6b2e-0000-4000-8000-000000000000")
        reasons = [ci_report.audit_violation(e, "helios") for e in entries]
        self.assertIn("type:DELETE with scope:resources", reasons[0])
        self.assertIsNotNone(reasons[1])
        self.assertEqual(reasons[2], "type:TRUNCATE with scope:resource_versions")
        self.assertIsNone(reasons[3])

    def test_another_keyspace_is_not_ours(self):
        [e] = ci_report.audit_records(entry("DELETE", "resources", "DELETE FROM other.resources WHERE a = 1", ks="other"))
        self.assertIsNone(ci_report.audit_violation(e, "helios"))

    def test_a_delete_naming_helios_from_another_keyspace_is_ours(self):
        # A failed, unprepared statement logs its session's keyspace, not the
        # one its text names (AuditLogManager.queryFailure, 5.0.9), so a
        # session using another keyspace can log a DELETE on helios.resources
        # under ks:other, with no scope (ADR-aacruzgon-0058's known limit).
        failed, quoted, batch, unqualified = ci_report.audit_records(
            entry("REQUEST_FAILURE", None, "DELETE FROM helios.resources WHERE tenant_id = 'x'; Operation timed out", ks="other", category="ERROR")
            + entry("REQUEST_FAILURE", None, 'DELETE FROM "helios"."resource_versions" WHERE a = 1; x', ks="other", category="ERROR")
            + entry("BATCH", None, "BEGIN BATCH DELETE FROM helios.resources WHERE a = 1; APPLY BATCH", ks="other")
            + entry("REQUEST_FAILURE", None, "DELETE FROM resources WHERE a = 1; x", ks="other", category="ERROR"))
        for e in (failed, quoted, batch):
            self.assertIsNotNone(ci_report.audit_violation(e, "helios"), e)
        self.assertIsNone(ci_report.audit_violation(unqualified, "helios"), "other.resources, not ours")

    def test_an_unprepared_table_wipe_is_caught_by_its_text(self):
        # A failed, unprepared statement has no scope (queryFailure, 5.0.9), so
        # rule (b')'s scope test cannot see a TRUNCATE or DROP TABLE that timed
        # out, and a timed-out TRUNCATE may still have applied on a replica.
        def failed(text, ks="helios"):
            return entry("REQUEST_FAILURE", None, f"{text}; Truncate timed out", ks=ks, category="ERROR")
        hits = ci_report.audit_records(
            failed("TRUNCATE resources")
            + failed("TRUNCATE TABLE resource_versions")
            + failed('truncate "helios"."resources"', ks="other")
            + failed("DROP TABLE IF EXISTS helios.resource_versions", ks="other")
            + failed("DROP TABLE resources"))
        for e in hits:
            self.assertIsNotNone(ci_report.audit_violation(e, "helios"), e)
        passes = ci_report.audit_records(
            failed("TRUNCATE resources_by_type")
            + failed("TRUNCATE pending_index_writes")
            + failed("TRUNCATE resources", ks="other")
            + failed("TRUNCATE other.resources", ks="other")
            + failed("DROP TABLE helios.resources_by_type", ks="other")
            + entry("UPDATE", "resources_by_type", "INSERT INTO resources_by_type (a) VALUES ('TRUNCATE resources')"))
        for e in passes:
            self.assertIsNone(ci_report.audit_violation(e, "helios"), e)

    def test_delete_text_sees_through_comments_and_odd_spacing(self):
        # Spellings the 5.0.9 parser accepts (each reached semantic validation
        # in cqlsh) that the text rule used to miss.
        def failed(text, ks="other"):
            return entry("REQUEST_FAILURE", None, f"{text}; Operation timed out", ks=ks, category="ERROR")
        hits = ci_report.audit_records(
            failed("DELETE FROM /* c */ helios.resources WHERE a = 1")
            + failed("DELETE FROM -- c\nhelios.resources WHERE a = 1")
            + failed("DELETE FROM // c\nhelios.resource_versions WHERE a = 1")
            + failed('DELETE FROM"helios"."resources" WHERE a = 1')
            + failed("DELETE m[';'] FROM helios.resources WHERE a = 1")
            + failed('DELETE "a;b" FROM helios.resources WHERE a = 1')
            + failed("DELETE FROM /* c */ resources WHERE a = 1", ks="helios")
            + failed("BEGIN BATCH INSERT INTO t (a) VALUES ('--'); DELETE FROM helios.resources WHERE a = 1; APPLY BATCH"))
        for e in hits:
            self.assertIsNotNone(ci_report.audit_violation(e, "helios"), e)
        # A literal or a comment naming such a DELETE is not one.
        passes = ci_report.audit_records(
            failed("INSERT INTO t (a) VALUES ('DELETE FROM helios.resources')")
            + failed("SELECT a FROM t /* DELETE FROM helios.resources */")
            + failed("SELECT a FROM t -- DELETE FROM helios.resources")
            + failed("INSERT INTO t (a) VALUES ($$DELETE FROM helios.resources$$)")
            + entry("UPDATE", "resources_by_type", "INSERT INTO resources_by_type (a) VALUES ('DELETE FROM resources; x')"))
        for e in passes:
            self.assertIsNone(ci_report.audit_violation(e, "helios"), e)

    def test_cql_code_blanks_literals_and_comments_only(self):
        self.assertEqual(ci_report.cql_code("SELECT 'it''s' FROM t -- x\nWHERE a = $$b$$ /* y */"),
                         "SELECT '' FROM t  \nWHERE a = $$$$  ")
        self.assertEqual(ci_report.cql_code('DELETE "a--b" FROM "x/*y"'), 'DELETE "a--b" FROM "x/*y"')
        self.assertEqual(ci_report.cql_code("DELETE FROM resources WHERE a = ?"), "DELETE FROM resources WHERE a = ?")

    def test_a_failed_native_batch_is_unverifiable(self):
        timed_out, no_keyspace, single = ci_report.audit_records(FAILED_BATCHES)
        self.assertEqual(timed_out["ks"], "helios")
        self.assertIsNone(no_keyspace["ks"])
        for e in (timed_out, no_keyspace):
            self.assertIsNone(ci_report.audit_violation(e, "helios"))
            self.assertIn("failed native-protocol batch", ci_report.audit_unverifiable(e, "helios"))
        self.assertIsNone(ci_report.audit_unverifiable(single, "helios"))
        # batchFailure logs the session's keyspace (AuditLogEntry.Builder(state),
        # 5.0.9), so a failed batch from a session on another keyspace may hold
        # DELETEs on helios.resources just the same: unverifiable too.
        other = ci_report.audit_records(entry("REQUEST_FAILURE", None, "BATCH of 2 statements at consistency ONE; x", ks="other", category="ERROR"))[0]
        self.assertIn("failed native-protocol batch", ci_report.audit_unverifiable(other, "helios"))


class Scratch(unittest.TestCase):
    """Each test writes its report into its own directory."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.env = {k: os.environ.get(k) for k in ("CI_REPORT_DIR", "GITHUB_STEP_SUMMARY")}
        os.environ["CI_REPORT_DIR"] = str(self.dir / "report")
        os.environ.pop("GITHUB_STEP_SUMMARY", None)

    def tearDown(self):
        for key, value in self.env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def write(self, name, text):
        path = self.dir / name
        path.write_text(text)
        return path

    def report(self, report_id):
        return json.loads((self.dir / "report" / f"{report_id}.json").read_text())["data"]

    def quiet(self, func, *args):
        with redirect_stdout(io.StringIO()) as out:
            status = func(*args)
        return status, out.getvalue()


class CqlAuditCommand(Scratch):
    def audit(self, text, check=True, name="audit.txt"):
        path = self.write(name, text) if text is not None else self.dir / "missing.txt"
        args = SimpleNamespace(id="cql-audit", title="CQL audit", file=str(path), keyspace="helios", check=check)
        return self.quiet(ci_report.cmd_cql_audit, args)

    def test_clean_log_passes(self):
        status, _ = self.audit(CLEAN)
        self.assertEqual(status, 0)
        data = self.report("cql-audit")
        self.assertTrue(data["check_passed"])
        self.assertEqual(data["violations"], [])
        self.assertIn({"table": "resources_by_type", "type": "DELETE", "count": 1}, data["by_table_and_type"])

    def test_delete_on_resources_fails_with_an_error_annotation(self):
        status, out = self.audit(RECORDED)
        self.assertEqual(status, 1)
        self.assertIn("::error title=CQL audit", out)
        self.assertEqual(len(self.report("cql-audit")["violations"]), 3)

    def test_a_delete_on_helios_logged_under_another_keyspace_fails_the_check(self):
        other = entry("REQUEST_FAILURE", None, "DELETE FROM helios.resources WHERE tenant_id = 'x'; Operation timed out", ks="other", category="ERROR")
        status, out = self.audit(CLEAN + other)
        self.assertEqual(status, 1)
        self.assertIn("::error title=CQL audit", out)
        self.assertEqual(len(self.report("cql-audit")["violations"]), 1)

    def test_a_failed_native_batch_from_another_keyspace_fails_the_check(self):
        other = entry("REQUEST_FAILURE", None, "BATCH of 400 statements at consistency ONE; Operation timed out", ks="other", category="ERROR")
        status, out = self.audit(CLEAN + other)
        self.assertEqual(status, 1)
        self.assertIn("cannot rule out a CQL DELETE", out)
        self.assertEqual(len(self.report("cql-audit")["unverifiable"]), 1)

    def test_an_unprepared_truncate_fails_the_check(self):
        wipe = entry("REQUEST_FAILURE", None, "TRUNCATE helios.resources; Truncate timed out", ks="other", category="ERROR")
        status, out = self.audit(CLEAN + wipe)
        self.assertEqual(status, 1)
        self.assertEqual(len(self.report("cql-audit")["violations"]), 1)

    def test_a_failed_native_batch_fails_the_check(self):
        status, out = self.audit(CLEAN + FAILED_BATCHES)
        self.assertEqual(status, 1)
        self.assertIn("cannot rule out a CQL DELETE", out)
        data = self.report("cql-audit")
        self.assertEqual((len(data["violations"]), len(data["unverifiable"]), data["failures"]), (0, 2, 3))

    def test_empty_or_missing_log_fails_the_check(self):
        self.assertEqual(self.audit("")[0], 1)
        self.assertEqual(self.audit(None)[0], 1)
        only_prepares = entry("PREPARE_STATEMENT", None, "SELECT 1", category="PREPARE")
        self.assertEqual(self.audit(only_prepares)[0], 1)

    def test_without_check_it_only_warns(self):
        status, out = self.audit(RECORDED, check=False)
        self.assertEqual(status, 0)
        self.assertIn("::warning", out)
        self.assertNotIn("::error", out)

    def test_gzip_input(self):
        import gzip

        path = self.dir / "audit.txt.gz"
        with gzip.open(path, "wt") as out:
            out.write(CLEAN)
        args = SimpleNamespace(id="cql-audit", title="CQL audit", file=str(path), keyspace="helios", check=True)
        self.assertEqual(self.quiet(ci_report.cmd_cql_audit, args)[0], 0)

    def test_plain_write_to_resources_and_allow_filtering_warn(self):
        text = CLEAN + entry("UPDATE", "resources", "INSERT INTO resources (a) VALUES (?)") + entry("UPDATE", "resources_by_type", "SELECT * FROM resources_by_type WHERE x = 1 ALLOW FILTERING")
        status, out = self.audit(text)
        self.assertEqual(status, 0)
        data = self.report("cql-audit")
        self.assertEqual((data["plain_writes_to_resources"], data["allow_filtering"]), (1, 1))
        self.assertIn("plain (non-IF) write", out)

    def test_multi_line_violation_is_one_line_in_the_report(self):
        status, _ = self.audit(CLEAN + entry("DELETE", "resources", "DELETE FROM resources\n  WHERE a = ?"))
        self.assertEqual(status, 1)
        self.assertEqual(self.report("cql-audit")["violations"][0]["operation"], "DELETE FROM resources WHERE a = ?")

    def test_corrupt_gzip_fails_the_check(self):
        bad = self.write("audit.txt.gz", "not gzip at all")
        result = subprocess.run([sys.executable, str(SCRIPT), "cql-audit", "cql-audit", "CQL audit", str(bad), "--check"],
                                env={**os.environ}, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn("could not run", result.stdout)

    def test_main_exit_status(self):
        bad = self.write("bad.txt", RECORDED)
        good = self.write("good.txt", CLEAN)
        env = {**os.environ}
        run = lambda *a: subprocess.run([sys.executable, str(SCRIPT), "cql-audit", "cql-audit", "CQL audit", *a], env=env, capture_output=True, text=True)
        self.assertEqual(run(str(bad), "--check").returncode, 1)
        self.assertEqual(run(str(good), "--check").returncode, 0)
        self.assertEqual(run(str(bad)).returncode, 0, "without --check it is a report")


class TaggedLines(unittest.TestCase):
    def test_pairs_quotes_and_colour(self):
        log = ('noise [cas case=latency-report:update attempts=100 applied=99 note="two words ]"]\n'
               "\x1b[32m[spec row=C-4 expected=201 got=201]\x1b[0m and [spec row=D-3 expected=204 got=404 step=7]\n")
        self.assertEqual(ci_report.tagged_lines(log, "cas"), [{"case": "latency-report:update", "attempts": "100", "applied": "99", "note": "two words ]"}])
        self.assertEqual(ci_report.tagged_lines(log, "spec"), [{"row": "C-4", "expected": "201", "got": "201"}, {"row": "D-3", "expected": "204", "got": "404", "step": "7"}])

    def test_a_weak_etag_is_one_value(self):
        # The Step 3 REST test prints ETags as they are, quotes included.
        log = '[spec row=CC-2:etag expected=W/"1" got=W/"2"]\n[spec row=X expected="a b" got=W/"x y"z]\n'
        self.assertEqual(ci_report.tagged_lines(log, "spec"), [
            {"row": "CC-2:etag", "expected": 'W/"1"', "got": 'W/"2"'},
            {"row": "X", "expected": "a b", "got": 'W/"x y"z'},
        ])


class SpecAndCasCommands(Scratch):
    def test_spec_mismatch_warns(self):
        log = self.write("step3-rest.log", "[spec row=U-7 expected=200 got=200]\n[spec row=D-3 expected=204 got=404]\n")
        status, out = self.quiet(ci_report.cmd_spec, SimpleNamespace(id="spec", title="HL7", log=str(log)))
        self.assertEqual(status, 0)
        data = self.report("spec")
        self.assertEqual((len(data["rows"]), data["mismatches"]), (2, 1))
        self.assertIn("D-3 expected 204, got 404", out)

    def test_spec_etag_mismatch_warns(self):
        log = self.write("step3-rest.log", '[spec row=CC-2:etag expected=W/"1" got=W/"2"]\n')
        status, out = self.quiet(ci_report.cmd_spec, SimpleNamespace(id="spec", title="HL7", log=str(log)))
        self.assertEqual((status, self.report("spec")["mismatches"]), (0, 1))
        self.assertIn('CC-2:etag expected W/"1", got W/"2"', out)

    def test_spec_missing_log_warns(self):
        status, out = self.quiet(ci_report.cmd_spec, SimpleNamespace(id="spec", title="HL7", log=str(self.dir / "none.log")))
        self.assertEqual(status, 0)
        self.assertIn("did not run", out)

    def test_cas_rows_and_warnings(self):
        log = self.write("latency-backend.log", "[cas case=a attempts=12 applied=10 undecidable=0]\n[cas case=b attempts=4 applied=4 miss_budget_exhausted=1]\n[cas-other case=c attempts=1]\n")
        status, out = self.quiet(ci_report.cmd_cas, SimpleNamespace(id="cas", title="CAS", logs=[str(log), str(self.dir / "absent.log")]))
        self.assertEqual(status, 0)
        data = self.report("cas")
        self.assertEqual([r["attempts_per_applied"] for r in data["rows"]], [1.2, 1.0])
        self.assertEqual(data["missing_logs"], ["absent.log"])
        self.assertIn("miss_budget_exhausted=1", out)
        self.assertNotIn("undecidable=0", out)

    def test_cas_fault_seam_rows_are_not_warned_on(self):
        lib = self.write("persistence-lib-tests.log", "[cas case=c2-undecidable attempts=3 applied=0 undecidable=1]\n")
        suites = self.write("persistence-tests.log", "[cas case=race:x attempts=9 applied=2 undecidable=0]\n"
                            "[cas case=race:y attempts=9 applied=2 miss_budget_exhausted=1]\n")
        latency = self.write("latency-backend.log", "[cas case=latency-report:update attempts=100 applied=100]\n")
        status, out = self.quiet(ci_report.cmd_cas, SimpleNamespace(id="cas", title="CAS", logs=[str(lib), str(suites), str(latency)]))
        self.assertEqual(status, 0)
        self.assertEqual(len(self.report("cas")["rows"]), 4)
        self.assertNotIn("c2-undecidable has", out)
        self.assertEqual(len(self.report("cas")["warnings"]), 1)
        self.assertIn("race:y has miss_budget_exhausted=1", out)
        self.assertNotIn("case in the logs", out)

    def test_cas_warns_on_a_missing_log_and_missing_cases(self):
        lib = self.write("persistence-lib-tests.log", "[cas case=lost-reply attempts=2 applied=1]\n")
        status, out = self.quiet(ci_report.cmd_cas, SimpleNamespace(id="cas", title="CAS", logs=[str(lib), str(self.dir / "latency-backend.log")]))
        self.assertEqual(status, 0)
        self.assertIn("no log latency-backend.log", out)
        self.assertIn("no `race:…` case", out)
        self.assertIn("no `latency-report:…` case", out)

    def test_cas_without_lines_says_so(self):
        log = self.write("x.log", "nothing\n")
        self.quiet(ci_report.cmd_cas, SimpleNamespace(id="cas", title="CAS", logs=[str(log)]))
        self.assertIn("no test printed", (self.dir / "report" / "cas.md").read_text())


class Intents(unittest.TestCase):
    def test_buckets_are_utc_hours(self):
        now = datetime(2026, 11, 7, 0, 30, tzinfo=timezone.utc)
        self.assertEqual(ci_report.intent_buckets(now, 3), ["2026-11-07T00", "2026-11-06T23", "2026-11-06T22"])

    def test_classify(self):
        self.assertEqual(ci_report.classify_intent({"version_id": 2}, None), "no resource row")
        self.assertEqual(ci_report.classify_intent({"version_id": 2}, {"version_id": 2}), "at or below the current version")
        self.assertEqual(ci_report.classify_intent({"version_id": 3}, {"version_id": 2}), "above the current version")

    def test_tenant_label(self):
        match = ci_report.TEST_TENANT.match("test-race-puts-0123456789abcdef0123456789abcdef")
        self.assertEqual(match.group(1), "race-puts")


class IntentsCommand(Scratch):
    def test_classes_labels_and_the_read_limit(self):
        hexes = "0123456789abcdef0123456789abcdef"
        intents = "\n".join(json.dumps(r) for r in [
            {"bucket": "b", "shard": 1, "tenant_id": f"test-race-{hexes}", "resource_type": "Patient", "id": "a", "version_id": 2},
            {"bucket": "b", "shard": 2, "tenant_id": f"test-race-{hexes}", "resource_type": "Patient", "id": "b", "version_id": 5},
            {"bucket": "b", "shard": 3, "tenant_id": f"test-crud-{hexes}", "resource_type": "Patient", "id": "c", "version_id": 1},
            {"bucket": "b", "shard": 4, "tenant_id": "acme", "resource_type": "Patient", "id": "d", "version_id": 1},
            {"bucket": "b", "shard": 5, "tenant_id": f"test-zz-{hexes}", "resource_type": "Patient", "id": "z", "version_id": 1},
        ])
        resources = "\n".join(json.dumps(r) for r in [
            {"tenant_id": f"test-race-{hexes}", "resource_type": "Patient", "id": "a", "version_id": 3, "deleted_at": 0},
            {"tenant_id": f"test-race-{hexes}", "resource_type": "Patient", "id": "b", "version_id": 4, "deleted_at": 0},
        ])
        calls = []

        def fake_run(command, timeout=60):
            calls.append(command[-1])
            return " [json]\n" + (intents if "pending_index_writes" in command[-1] else resources) + "\n"

        original = ci_report.run
        ci_report.run = fake_run
        try:
            args = SimpleNamespace(id="intents", title="Intents", container="c", keyspace="helios", tenant_prefix="test-", hours=3, limit=3)
            self.assertEqual(self.quiet(ci_report.cmd_intents, args)[0], 0)
        finally:
            ci_report.run = original
        data = self.report("intents")
        self.assertEqual(data["intents"], 4, "acme is not a test tenant")
        self.assertEqual(data["by_test"]["race"], {"at or below the current version": 1, "above the current version": 1})
        self.assertEqual(data["by_test"]["crud"], {"no resource row": 1})
        self.assertEqual(data["by_test"]["zz"], {"not read (over --limit)": 1})
        self.assertIn("shard IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)", calls[0])
        self.assertEqual(calls[1].count("SELECT JSON"), 3, "one point read per resource, up to --limit")


    HEXES = "0123456789abcdef0123456789abcdef"

    def intents_with(self, n, resources_answer, limit=500):
        """cmd_intents over n intents (versions 1), with the resources reads answered by
        resources_answer(statement_text) -> stdout or None. Returns (data, output, calls)."""
        rows = [{"bucket": "b", "shard": k % 16, "tenant_id": f"test-crud-{self.HEXES}", "resource_type": "Patient",
                 "id": f"p{k:04d}", "version_id": 1} for k in range(n)]
        calls = []

        def fake_run(command, timeout=60):
            if command[:2] != ["docker", "exec"]:
                return None  # the report's own context commands
            calls.append(command[-1])
            if "pending_index_writes" in command[-1]:
                return " [json]\n" + "\n".join(json.dumps(r) for r in rows) + "\n"
            return resources_answer(command[-1])

        original = ci_report.run
        ci_report.run = fake_run
        try:
            args = SimpleNamespace(id="intents", title="Intents", container="c", keyspace="helios", tenant_prefix="test-", hours=3, limit=limit)
            status, out = self.quiet(ci_report.cmd_intents, args)
        finally:
            ci_report.run = original
        self.assertEqual(status, 0)
        return self.report("intents"), out, calls

    @staticmethod
    def found(statements):
        """A resources read that finds every resource it asks for, at version 2."""
        ids = re.findall(r"id = '(p\d+)'", statements)
        return " [json]\n" + "\n".join(json.dumps({"tenant_id": f"test-crud-{IntentsCommand.HEXES}", "resource_type": "Patient",
                                                    "id": i, "version_id": 2, "deleted_at": 0}) for i in ids) + "\n"

    def test_a_failed_resources_read_is_not_read_and_warns(self):
        data, out, _ = self.intents_with(3, lambda q: None)
        self.assertEqual(data["by_class"], {"not read (resources read failed)": 3})
        self.assertEqual(len(data["warnings"]), 1)
        self.assertIn("resources could not be read from c", data["warnings"][0])
        self.assertIn("::warning", out)

    def test_the_resources_reads_are_chunked(self):
        data, out, calls = self.intents_with(250, self.found)
        reads = [c for c in calls if "pending_index_writes" not in c]
        self.assertEqual([r.count("SELECT JSON") for r in reads], [100, 100, 50])
        self.assertTrue(all(len(r.encode()) < 128 * 1024 for r in reads), "each -e argument under MAX_ARG_STRLEN")
        self.assertEqual(data["by_class"], {"at or below the current version": 250})
        self.assertEqual(data["warnings"], [])

    def test_a_failed_middle_chunk_marks_only_its_own_intents(self):
        answers = iter([self.found, lambda q: None, self.found])
        data, out, _ = self.intents_with(250, lambda q: next(answers)(q))
        self.assertEqual(data["by_class"], {"at or below the current version": 150, "not read (resources read failed)": 100})
        self.assertEqual(len(data["warnings"]), 1)
        self.assertIn("1 of 3 read(s) failed", data["warnings"][0])
        self.assertIn("100 resource(s)", data["warnings"][0])
        failed = sorted(r["id"] for r in data["rows"] if r["class"].startswith("not read"))
        self.assertEqual(failed, [f"p{k:04d}" for k in range(100, 200)], "the second chunk of the sorted keys")


class CrudTombstones(unittest.TestCase):
    def test_soft_delete_tables_warn_and_resources_by_type_does_not(self):
        rows, md, warnings = ci_report.crud_tombstone_rows({
            "resources": {"count": 10, "max": 0, "p99th": 0},
            "resource_versions": {"count": 5, "max": 2.0, "p99th": 1.0},
            "resources_by_type": {"count": 9, "max": 40.0, "p99th": 30.0},
        })
        self.assertEqual([r["table"] for r in rows], ["resources", "resource_versions", "resources_by_type"])
        self.assertEqual(len(warnings), 1)
        self.assertIn("resource_versions", warnings[0])
        self.assertIn("`resources_by_type`", md)

    def test_the_histogram_floor_of_1_is_not_a_tombstone(self):
        # Recorded on CI run 42336353732: no DELETE in the audit log, yet both
        # soft-delete tables read max 1.0, the histogram's lowest bucket.
        _, _, warnings = ci_report.crud_tombstone_rows({
            "resources": {"count": 24103, "max": 1.0, "p99th": 1.0},
            "resource_versions": {"count": 5, "max": 1.0, "p99th": 1.0},
        })
        self.assertEqual(warnings, [])

    def test_missing_tables_are_blank(self):
        rows, _, warnings = ci_report.crud_tombstone_rows({})
        self.assertEqual(warnings, [])
        self.assertIsNone(rows[0]["max"])


if __name__ == "__main__":
    unittest.main()
