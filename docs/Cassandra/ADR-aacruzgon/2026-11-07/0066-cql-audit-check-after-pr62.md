# ADR-aacruzgon-0066: The CQL audit check after PR #62: each rule also reads the CQL text, across keyspaces, and the audit dump is read fail-closed

- **Date:** 2026-11-07
- **Status:** accepted (Alan chose a new entry over 0058's Status line on 2026-11-07, as recommended, accepted its two fail-closed false failures the same day, and approved this entry on 2026-11-07). It amends ADR-aacruzgon-0058's matching rules and its known limits.
- **Context:**
    - **What ADR-0058 decided.** The required Cassandra job turns the node's audit log on, dumps it with `auditlogviewer`, and `ci-report.py cql-audit --check` fails the job on any of these:
        - **(a)** a `type:DELETE` entry scoped exactly to `resources` or `resource_versions`;
        - **(b)** a `DELETE … FROM` either table in an entry's CQL text;
        - **(b′)** a `TRUNCATE` or `DROP_TABLE` entry with either scope;
        - **(c)** an empty log, or one with no DML in `helios`;
        - **(d)** a failed native-protocol batch, which is unverifiable.

      The rules applied only to entries whose `ks` is `helios` or empty. ADR-0058 accepted three known limits:
        - entries logged under another keyspace were not checked;
        - rule (b) was not tied to a statement's start, so a literal or comment naming a DELETE failed the check falsely;
        - a failed native batch fails the check even when it is legitimate.
    - **What PR #62's audits found.** PR #62 (branch `cassandra/step3-cleanup`) set out to close the first limit. Its three independent audits found that ADR-0058's rules were weaker than written. Each finding was reproduced on the parser, against the 5.0.9 server's lexer, parser and messages, or on a 5.0.9 node of the reviewer's own:
        - **A failed native batch logs the session's keyspace.** `AuditLogManager.batchFailure` builds its entry from `AuditLogEntry.Builder(state)`, the session's keyspace (5.0.9, `AuditLogManager.java:305-312`, `AuditLogEntry.java:193-211`). So rule (d) skipped a batch from a session on another keyspace, which may hold DELETEs on `helios.resources`.
        - **A failed, unprepared `TRUNCATE` or `DROP TABLE` has no scope** (`queryFailure`, `:254-260`), so rule (b′), which matched on scope only, missed it from any keyspace. A timed-out `TRUNCATE` may still have applied on a replica.
        - **Rule (b) missed spellings that the 5.0.9 parser accepts:**
            - a comment between `FROM` and the table;
            - a quoted name right after `FROM`, with no space;
            - a `;` before `FROM`, in a literal or a quoted identifier.
        - **`COLUMNFAMILY` is `TABLE`'s synonym** in `TRUNCATE` and `DROP` (`Parser.g:1040`, `:1073`; `Lexer.g:94` defines `K_COLUMNFAMILY`).
        - **The dump reader trusted every header-looking line.** `auditlogviewer` prints each record as `Type: audit` and `LogMessage: <entry>` (`AuditLogViewer.java`, `DisplayRecord`). An entry's CQL text is free text, so a line in it can look like a header.
            - Before `fc6396036`, such a line inside a literal split the record, and the DELETE after it went unchecked.
            - On a 5.0.9 node, a CQL-text batch with such a header inside a literal and a real `DELETE FROM resources` ran, and the row was deleted. The check passed that dump.
    - **Why it is a decision.**
        - It changes what the required check accepts and rejects, so it changes CI.
        - It amends an accepted entry.
        - It chose between workable designs: how far to trust the dump's framing, and which false failures to accept.

      The log is append-only, and an amendment goes in its own entry, as ADR-aacruzgon-0055 amends 0050 (`ADR-aacruzgon.md`, "Rules").
- **Decision:**
    - **What each rule catches now**, with the commit that made it so. All are on PR #62.
        - **(a)** Unchanged: `type:DELETE` with a scope of exactly `resources` or `resource_versions`.
        - **(b)** A `DELETE … FROM` either table in the CQL text.
            - **How the text is read.** Every text rule runs on `cql_code(text)`, which reads the text as the 5.0.9 lexer does. It blanks comments (`/* */`, and `--` or `//` to the end of the line, which a CR or an LF ends) and empties string literals (`'…'` with `''` escapes, and `$$…$$`), keeping quoted names (`22f3f58ae`; the CR rule in `fc6396036`).
            - **What that fixes.** A comment, odd spacing, `FROM"…"`, or a `;` in a literal or quoted name no longer hides a DELETE (`22f3f58ae`). A literal or comment that only names one is no longer a false failure.
            - **Another keyspace.** An entry logged under another keyspace is checked when its text names a `helios.`-qualified table (`093ca1233`).
        - **(b′)** A `TRUNCATE` or `DROP` of either table: by scope, as before, and also by its text.
            - The spellings matched are `TRUNCATE [TABLE|COLUMNFAMILY]` and `DROP TABLE|COLUMNFAMILY [IF EXISTS]` (`22f3f58ae`, `5387007fe`).
            - Another keyspace's entry is checked under the same qualified-name rule as (b) (`22f3f58ae`).
        - **(c)** Unchanged.
        - **(d)** A failed native batch is unverifiable whatever keyspace it is logged under (`2ddcb663d`).
        - **(e) New: the dump is read fail-closed.**
            - **When a record starts** (`fc6396036`): only at a `Type: audit` line followed by a well-formed `LogMessage:` line. Lines are split on `\n` only, so a bare CR stays in the text.
            - **When a header is trusted** (`95fde3bc4`): only if its user, host and source fields hold no quote, `$$`, `/*`, `--` or `//`. The user field is the part before the header's first `|`.
            - **A record is unverifiable, and fails the check, when:**
                - its text holds another line that starts like a header; or
                - its text ends inside a literal, quoted name or block comment while another record follows; or
                - it is text before the first record (`fc6396036`).
            - **The exception for a failed statement** (`95fde3bc4`): a failure's text is its query, `"; "`, then the server's message (`AuditLogEntry.Builder.appendToOperation`), and the message can leave a quote open. So for a `REQUEST_FAILURE`, `failure_query_end` finds where the query ended. That is a `"; "` that is code, not in a literal, quoted name or comment, followed by a word no statement that ran could continue with, that is, not `INSERT`, `UPDATE`, `DELETE`, `APPLY` or `BEGIN`. An open quote after that point is in the message, and does not make the record unverifiable.
    - **Why a trusted header cannot hide inside a statement that ran** (`fc6396036`, `95fde3bc4`, `8c2ca443a`). The proof is the 5.0.9 lexer together with the header-field rule. The parser only backs it up.
        - **Lexer, with the header-field rule.** `|` is no CQL token, and any lexer error rejects the statement (`CQLFragmentParser.parseAnyUnhandled`). Before its first `|`, a trusted header line opens nothing, because its user field holds no quote, `$$`, `/*`, `--` or `//`. So the first `|` is code, unless a literal, quoted name or block comment opened before the header, and then the text before the header ends inside one. A `|` in code is a lexer error, so a header in the code of a statement frames a false record only from a statement that failed to parse, which never ran.
        - **Parser, as evidence only.** Read as code, the tokens every header starts with, `Type : audit LogMessage : user :`, were a syntax error on the 5.0.9 server in each of ten contexts tried: after a batch child, in an `INSERT`'s map or UDT literal, in a select-list map or UDT literal, after `= :`, in a select list, in `IN (`, after `USING`, in a column list, in `ALTER`, and in a `WITH` map. The first line alone is valid CQL. A select-list map or UDT literal may hold column names, so `SELECT (map<text, text>){Type: audit} FROM s` parses: 5.0.9 answers it with an undefined-column error, not a syntax error. What fails is `LogMessage` after `audit` with no comma.
    - **What does not change.**
        - The node's configuration and the workflow steps: `enableauditlog` with the system keyspaces excluded, the dump, and `--check`.
        - Report R7.
        - The report-only counts.
- **Alternatives considered:**
    - **Keep ADR-0058's rules and its other-keyspace limit:** lost. The limit was accepted only for a later CI pass. The audits then found misses inside `helios` as well: the unprepared `TRUNCATE`, the spellings, and the framing.
    - **Record the amendment in 0058's Status line, as `2ddcb663d` and `bdb5fc38c` did:** lost. It puts new decisions where the template has a status, with no Context, Alternatives or AI involvement for them. It also departs from the 0050 → 0055 precedent. Alan chose the new entry on 2026-11-07.
    - **Rule (d) only for `helios` and keyspace-less entries:** lost, because `batchFailure` logs the session's keyspace.
    - **Text rules on the raw text, as ADR-0058 first did:** lost. A comment or odd spacing hid a DELETE, which is a miss, and a literal naming one failed the check falsely.
    - **Trust every `Type:`/`LogMessage:` line, the original reader:** lost. A header inside a literal hid a DELETE that applied, verified on a 5.0.9 node.
    - **Fail on every record that ends open, as `fc6396036` did:** lost. Three recorded 5.0.9 messages leave a quote open:
        - `no viable alternative at input 'it's'`;
        - `Invalid STRING constant (it's) for "version_id" of type int`;
        - the same for a uuid.

      Each failed the required check for a statement that never ran.
    - **End the query at any closed `"; "` followed by a word:** lost. A `"; word"` inside a `--` comment reopened a miss, the delta audit's case A7. The rule requires the `"; "` to be code.
    - **Stop excluding the system keyspaces in `enableauditlog`, which would close the one gap left:** not taken now. It is a workflow change, and its cost in entries and time is unmeasured. Alan chose on 2026-11-07 to revisit it only if a CI step starts using another keyspace.
    - **A dump format with unambiguous framing:** not taken. `auditlogviewer` is the only reader in the image, and its output format is fixed. Reading the Chronicle files another way would need a tool the image does not have. The fail-closed reader covers the framing, and Alan chose on 2026-11-07 not to investigate.
- **Consequences:**
    - **What the check guarantees.** A CQL `DELETE`, `TRUNCATE` or `DROP` of `resources` or `resource_versions` fails the job, prepared or not, failed or not, in a batch or not, under `helios` or under another keyspace that names `helios.`. So does a record the reader cannot frame unambiguously.
    - **What stays open:**
        - **Entries under an excluded keyspace are never written.** The node drops an entry whose keyspace is excluded (`AuditLogFilter.isFiltered`, 5.0.9). So a failed unprepared statement or batch, sent from a session using a system keyspace, leaves nothing to check. No CI session uses one today. Only a change to the job's `--excluded-keyspaces` could close it.
        - **`DROP KEYSPACE helios`** is not a rule (b′) case. It is outside ADR-0058's scope, and it would break every later step anyway.
    - **The accepted false failures,** each of which fails closed. Alan accepted the truncated-password and the unresolved-quote cases on 2026-11-07, next to case folding:
        - **Quoted names fold case.** `"Helios"` and `"Resources"` match, though in CQL they are another keyspace and another table.
        - **The truncated-password query.** A query holding "password" that fails to parse or to prepare is logged cut after that word, inside its literal (`PasswordObfuscator.obfuscate`, through `QueryEvents.maybeObfuscatePassword`). Preparing includes validation, such as a wrong-typed literal, and `QueryMessage.execute` has no statement until both succeed. It is unverifiable either way when any record follows. The syntax-error case was recorded on a 5.0.9 node. The validation-failure case is built from the 5.0.9 source (`QueryProcessor.getStatement`).
        - **A failed statement whose message leaves a quote open where the guard cannot find the query's end.** `failure_query_end` resolves the three recorded messages. It finds no end, and the record stays unverifiable when another record follows, if the query itself ends open (the password case), or if no `"; "` that is code is followed by a word other than `INSERT`, `UPDATE`, `DELETE`, `APPLY` or `BEGIN`: for example a message that starts with a quote or a digit, or whose first word is one of those five. None of the messages recorded on 5.0.9 starts that way; the case is accepted so that the guard never trusts what it cannot place.
        - **A role name with a quote, `$$`, `/*`, `--` or `//`** makes its own headers untrusted. CI's user is `anonymous`.
        - **A failed native batch,** as ADR-0058 accepted.

      An unterminated literal is not one: its 5.0.9 message closes it again.
    - **Measured** on the real dumps:
        - Run 42401552753 (32,652 entries) and run 42416765752 (32,815): 0 violations and 0 unverifiable, both before and after these changes.
        - Parsing run 42401552753's dump went from 0.1 s to 0.5 s, from one local run of each.
    - **Tests.** `.github/scripts/test_ci_report.py` has 61 tests. They include:
        - every framing attack of PR #62's second delta audit, each of which must fail the check;
        - the three recorded messages, byte for byte, and the first one's earlier `['it''s']` form, all of which must pass;
        - the truncated-password query, after a syntax error and after a validation failure, which must stay unverifiable;
        - fakes that open a comment or literal in a header's user field.
    - **To revisit:**
        - if a CI step ever uses another keyspace, or sends native batches;
        - if CI turns on authentication, which changes the user field;
        - at Cassandra upgrades, since `AuditLogViewer`, `AuditLogEntry.getLogString`, `Lexer.g` and `Parser.g` are what the reader relies on.
- **AI involvement:**
    - **Tools and models:** Claude Code (Opus 5.5) workers, driven by an orchestrating session.
    - **PR #62's author** (a Claude Code worker) wrote `093ca1233`: rule (b) for another keyspace, ADR-0058's "one-line hardening".
    - **The first independent audit,** worker `pr62-review`, audited PR #62 and found the gaps:
        - rule (d) for another keyspace;
        - the unprepared `TRUNCATE`;
        - the three DELETE spellings;
        - that 0058's Status line claimed more than the code did.

      It reproduced them with parser probes and cqlsh parses against a keyspace that does not exist, and fixed them in `22f3f58ae` and `2ddcb663d`.
    - **A second reviewer,** a different worker, audited those fixes and added four commits:
        - `5387007fe`: `COLUMNFAMILY`;
        - `fc6396036`: the fail-closed reader, after reproducing the framing miss on a 5.0.9 node;
        - `deb500a40`: comments;
        - `bdb5fc38c`: 0058's Status line.
    - **The second delta audit,** by `pr62-review`, audited those four. It reproduced the framing miss independently on its own node, Compose project `review-audit`, removed afterwards. It found two things:
        - the broader false-failure mode, from the three server messages recorded on the shared node and the truncated-password case recorded on its node;
        - the gap in the lexer-only soundness argument, a comment opened in a fake header's user field, which passed the check at `bdb5fc38c`.

      It prototyped the guarded `"; "` rule in scratch, found that the naive version missed case A7, and built the guarded one in `95fde3bc4`.
    - **An independent audit of `95fde3bc4`** found that the `"; "` rule hid nothing in any case, tested live or on the parser. It made three wording findings:
        - the parser argument was too strong, since `{Type: audit}` parses in a select list;
        - the password cut also happens on a validation failure;
        - the first recorded message's fixture was not byte-exact.

      `pr62-review` checked the first on the shared node, read the second in the 5.0.9 source, and fixed all three in `8c2ca443a` and in this entry, after Alan's approval of 2026-11-07. The code is unchanged.
    - **This entry:** both reviewers recommended moving the amendment out of 0058's Status line into a new entry, and accepting the fail-closed false failures. `pr62-review` drafted this one, and wrote in Alan's approvals of 2026-11-07.
    - **Verified:**
        - **Source read at tag `cassandra-5.0.9`:** `AuditLogManager`, `AuditLogEntry`, `AuditLogFilter`, `AuditLogViewer`, `Lexer.g`, `Parser.g`, `CQLFragmentParser`, `QueryEvents`, `PasswordObfuscator`, and the transport messages.
        - **Raw statements sent to the shared node,** only to a keyspace that does not exist, or as failing or read-only SELECTs: the lexer's parity with `cql_scan`, the ten grammar contexts, `{Type: audit}` in a select list, and the messages.
        - **On the reviewer's own node:** the batch whose DELETE applied, with the old reader passing that dump and the new reader failing it.
        - **The test suite and both real dumps;** the R4B check; CI's five required checks on each pushed head.
    - **Not verified:**
        - that `Type : audit LogMessage : user :` is invalid in *every* CQL context; that was tested in ten;
        - the system-keyspace gap on a node;
        - an execution failure whose message echoes user input. None was found in the 5.0.9 source read, and user-defined functions are off.
    - **What Alan changed:** on 2026-11-07 he chose the new-entry approach, so 0058's Status line goes back to its original text plus "; amended by ADR-aacruzgon-0066", and he accepted both fail-closed false failures: a failed statement whose message leaves a quote open that the guard cannot resolve, and the truncated-password query. On the same day he settled the two open alternatives: the system keyspaces stay excluded, revisited only if a CI step starts using another keyspace, and no other dump format is investigated, since the fail-closed reader covers the framing. Otherwise he approved the entry as drafted, on 2026-11-07.
- **Links:**
    - PR #62, branch `cassandra/step3-cleanup`: commits `093ca1233`, `22f3f58ae`, `2ddcb663d`, `5387007fe`, `fc6396036`, `95fde3bc4`, `8c2ca443a`; this entry's commit;
    - ADR-aacruzgon-0058 (amended here), and 0050 → 0055 (the precedent);
    - `.github/scripts/ci-report.py` (`cql-audit`: `audit_records`, `cql_scan`, `failure_query_end`, `protected_statements`, `audit_violation`, `audit_unverifiable`), `.github/scripts/test_ci_report.py`;
    - `.github/workflows/fmt-clippy-cassandra.yml`, job `cassandra`, steps "Turn on the CQL audit log" and "Check the CQL audit log …", which are unchanged;
    - Cassandra 5.0.9: `src/java/org/apache/cassandra/audit/{AuditLogManager,AuditLogEntry,AuditLogFilter}.java`, `src/java/org/apache/cassandra/tools/AuditLogViewer.java`, `src/antlr/{Lexer,Parser}.g`, `src/java/org/apache/cassandra/cql3/{CQLFragmentParser,QueryEvents,PasswordObfuscator}.java`.
