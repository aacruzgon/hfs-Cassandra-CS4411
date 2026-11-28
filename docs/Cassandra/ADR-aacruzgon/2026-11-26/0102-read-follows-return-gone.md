# ADR-aacruzgon-0102: A read, HEAD or batch GET of a deleted resource follows `HFS_RETURN_GONE`, as vread does, since upstream #2583

- **Date:** 2026-11-26
- **Status:** accepted; supersedes ADR-aacruzgon-0064 item 11
- **Context:**
    - **What 0064 item 11 recorded.** On 2026-11-06, `HFS_RETURN_GONE=false` changed vread only (D18, matrix DS-1). A read of a deleted resource answered 410 whatever the flag, and so did a HEAD and a batch GET. That was REST's behaviour, not Cassandra's: `read_handler` passed `ResourceError::Gone` straight through `From<ResourceError>`. 0064 filed it upstream as HeliosSoftware/hfs#2526, did not fix it in the fork (D18 b lost), and corrected the walkthrough to say "vread only".
    - **What changed.** Upstream merged HeliosSoftware/hfs#2583 (`4cb18e842`, "honor HFS_RETURN_GONE across deleted reads") on 2026-11-26, and it closed #2526.
        - It adds `RestError::gone_as_configured` (`crates/rest/src/error.rs`), which turns `Gone` into `NotFound` when the flag is false.
        - `read_handler`, `head_read_handler` (`crates/rest/src/handlers/read.rs`) and the batch GET entry (`crates/rest/src/handlers/batch.rs`) call it.
        - The fork takes it with the upstream sync, PR #134 (`sync/upstream-2026-11-26`, head `38cdf9c3f`).
    - **Measured through REST on Cassandra** (5.0.9, one node, `hfs` built R4+R5 with `cassandra`, no other change):

      | Request on a deleted resource | `HFS_RETURN_GONE` | R4 and R5 at #134 `38cdf9c3f` | R4 and R5 at `main` `eeb61fc56` |
      | --- | --- | --- | --- |
      | `GET /Patient/{id}` | default | 410 `deleted` | 410 `deleted` |
      | `GET /Patient/{id}` | false | 404 `not-found` | 410 `deleted` |
      | `HEAD /Patient/{id}` | default | 410 | 410 |
      | `HEAD /Patient/{id}` | false | 404 | 410 |
      | batch `GET Patient/{id}` | default | 410 `deleted` | 410 `deleted` |
      | batch `GET Patient/{id}` | false | 404 `not-found` | 410 `deleted` |

        - The controls answered the same at both commits: an unknown id 404, a live one 200, and vread of the marker 410 by default and 404 with the flag false.
        - CQL showed each deleted row in `resources` with `deleted_at > 0`. The backend's answer is unchanged (`ResourceError::Gone`); only REST's mapping moved.
    - **The spec.**
        - **R4 and R4B** ([http.html#read](https://hl7.org/fhir/R4/http.html#read)) say, descriptively: "a GET for a deleted resource returns a 410 status code, whereas a GET for an unknown resource returns 404. Systems that do not track deleted records will treat deleted records as an unknown resource." R4B's text is the same.
        - **R5** ([http.html#delete](https://hl7.org/fhir/R5/http.html#delete)) makes 410 the server's choice: reads "return a 410 Gone HTTP status code when the server wishes to indicate that the resource is deleted. For security reasons, the server may return other status codes as defined under Access Denied Response Handling."
        - None of the three has a SHALL. The default, 410, is what each describes. 404 with the flag false is the "do not track deleted records" case in R4 and R4B, and allowed outright in R5.
- **Decision:**
    - The fork follows upstream.
        - A read, HEAD or batch GET of a deleted resource answers 410 when `HFS_RETURN_GONE` is true, the default, and 404 `not-found` when it is false, as vread already did.
        - Cassandra's `read` keeps returning `ResourceError::Gone`, and there is no Cassandra code change.
    - 0064 item 11 is superseded. Its documents, which said "vread only", are corrected again in this entry's commit:
        - `walkthrough.md` section 2 (the `read` row) and section 4.1;
        - roadmap D18 and DS-1;
        - `schema/README.md`'s 0064 row;
        - the `read.rs` module doc;
        - this log's 0027 and 0064 index rows, and 0064's status line.
    - ADR-aacruzgon-0027 `:167` ("410, or 404 with `HFS_RETURN_GONE` false") is true again and stays as written.
- **Alternatives considered:**
    - **Keep "410 whatever the flag" in the fork by reverting #2583's three call sites:** lost.
        - It would carry a fork patch against upstream's REST layer, for a behaviour upstream has just called a bug (#2526).
        - The R5 text and R4's "do not track deleted records" sentence both allow 404 when an operator asks for it, and the default is unchanged.
    - **Leave the documents as they are, since the default didn't change:** lost. Four documents and a doc comment would say something false about `HFS_RETURN_GONE=false`, the mistake 0064 item 11 was written to fix.
- **Consequences:**
    - **No test changes.** `step3_crud_through_rest` asserts 410 with the default flag (`R-3:read-deleted`, `G13:head-deleted`, `D-4:still-gone`, `B-7:get-deleted`, `V-1:vread-marker`, `crates/hfs/tests/cassandra_milestone_a.rs`), and those hold.
    - **No Cassandra test runs with the flag false.** Upstream's `crates/rest/tests/return_gone_reads.rs` covers both flag states on upstream's test backend. Adding a flag-false case to `step3_crud_through_rest` is an option, not part of this entry.
    - **PATCH of a deleted resource still answers 410 with the flag false** (measured at `38cdf9c3f`, R4 and R5; matrix CC-11, `cassandra_milestone_a.rs:926-935`).
        - #2583 does not touch the PATCH handler, so a client that set the flag to false sees 404 on a read and 410 on a PATCH of the same resource.
        - It is REST's, the same on every backend, and left to upstream. R4's #patch doesn't name a status for a deleted target.
    - Revisit if upstream changes `gone_as_configured`, or extends it to PATCH or conditional operations.
- **AI involvement:**
    - The `validate-109-findings` worker (Claude Code, Opus 5.5), in its follow-up 1 for the orchestrator `hfs-cassandra-cs4411-f5`, reproduced the finding of the #134 audit, working from the claim alone.
    - It built `hfs` at both commits.
    - It ran `return-gone/probe.sh` for R4 and R5, each with the flag at its default and false, on its own Cassandra 5.0.9 node.
    - It checked the stored rows with CQL.
    - It fetched R4, R4B and R5 `http.html` on 2026-11-26 and quoted #read and #delete.
    - It searched `docs/Cassandra/` and `crates/persistence/src/backends/cassandra/` for every statement about `HFS_RETURN_GONE` and 410.
    - It drafted this entry and the replacement lines. Alan reviews and confirms every line before it is committed.
- **Links:**
    - HeliosSoftware/hfs#2583 (`4cb18e842`), which closed HeliosSoftware/hfs#2526; fork PR #134.
    - ADR-aacruzgon-0064 item 11 (superseded), and ADR-aacruzgon-0027 `:167`.
    - `docs/Cassandra/schema/walkthrough.md` section 2 ("`deleted_at` and its readers") and 4.1.
    - `docs/Cassandra/development-roadmap.md`: Step 3 decisions D18 and HL7 matrix DS-1.
    - `docs/Cassandra/schema/README.md`, "Changes made to the design".
    - `crates/persistence/src/backends/cassandra/read.rs`.
