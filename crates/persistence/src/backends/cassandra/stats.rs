//! The write engine's outcome counters, [`CasStats`] (Step 3 CI report R2,
//! R3 and R8; `docs/Cassandra/development-roadmap.md` Step 3).
//!
//! One set of counters per backend, bumped by the engine in `write.rs` with
//! relaxed atomic adds: a few per compare-and-swap, which costs nothing next
//! to a Paxos round. A test or a report takes a snapshot with
//! [`CassandraBackend::cas_stats`](super::CassandraBackend::cas_stats)
//! before and after what it measures, and prints [`CasStats::since`] as
//! `key=value` pairs (its `Display`).
//!
//! The five extraction and payload counters at the end of [`CasStats`] are
//! the writer's step 1 (roadmap row 5.1).
//!
//! The statements themselves are counted per kind in `session.rs`
//! ([`StatementCounts`](super::StatementCounts), report R5).
//!
//! [`IndexWriteStats`] counts what one write's index phase packed: its
//! planned rows, its batches and the largest of them (roadmap Step 5;
//! ADR-sam-michaels-0006, IW14).

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use cdrs_tokio::error::Error as CdrsError;
use cdrs_tokio::frame::message_error::{ErrorType, WriteType};

use super::session::DriverError;

macro_rules! cas_counters {
    ($($(#[doc = $doc:literal])* $name:ident,)*) => {
        /// The engine's counters, one atomic per field of [`CasStats`].
        #[derive(Debug, Default)]
        pub(crate) struct CasCounters {
            $($name: AtomicU64,)*
        }

        impl CasCounters {
            /// A snapshot of every counter.
            pub(crate) fn snapshot(&self) -> CasStats {
                CasStats {
                    $($name: self.$name.load(Ordering::Relaxed),)*
                }
            }
        }

        /// A snapshot of the write engine's counters since the backend was
        /// built. Every field only grows, so two snapshots subtract
        /// ([`CasStats::since`]).
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        #[non_exhaustive]
        pub struct CasStats {
            $($(#[doc = $doc])* pub $name: u64,)*
        }

        impl CasStats {
            /// What happened between `earlier` and `self`.
            pub fn since(&self, earlier: &CasStats) -> CasStats {
                CasStats {
                    $($name: self.$name.saturating_sub(earlier.$name),)*
                }
            }

            /// Every counter as `(name, value)`, in declaration order.
            pub fn pairs(&self) -> Vec<(&'static str, u64)> {
                vec![$((stringify!($name), self.$name),)*]
            }
        }
    };
}

cas_counters! {
    /// Compare-and-swaps sent: every attempt, re-runs included.
    attempts,
    /// Requests whose write applied: by the CAS's own answer, by the serial
    /// read, or by a later look at the row (ADR-aacruzgon-0051).
    applied,
    /// Of `applied`, those where an earlier attempt of the request, not the
    /// last one sent, turned out to be the one that applied.
    applied_late,
    /// Of `applied`, those already superseded by a later version, so the
    /// version and index rows were left to the sweeper.
    applied_superseded,
    /// Definite misses: `[applied]` alone (the row never existed).
    miss_no_row,
    /// Definite misses: the row was deleted (`deleted_at > 0`).
    miss_deleted,
    /// Definite misses: another version holds the row.
    miss_version_conflict,
    /// Definite misses: `IF NOT EXISTS` found a row.
    miss_already_exists,
    /// Retries an operation's loop took after a definite miss.
    miss_retries,
    /// Requests whose definite-miss budget ran out.
    miss_budget_exhausted,
    /// Unknown outcomes: `WriteTimeout` with `write_type = CAS`.
    unknown_write_timeout_cas,
    /// Unknown outcomes: `WriteTimeout` with `write_type = SIMPLE`
    /// (ADR-aacruzgon-0035).
    unknown_write_timeout_simple,
    /// Unknown outcomes: `WriteTimeout` with another `write_type`.
    unknown_write_timeout_other,
    /// Unknown outcomes: `WriteFailure`.
    unknown_write_failure,
    /// Unknown outcomes: `ReadTimeout`.
    unknown_read_timeout,
    /// Unknown outcomes: `ReadFailure`.
    unknown_read_failure,
    /// Unknown outcomes: `CAS_WRITE_UNKNOWN` (legacy Paxos v1).
    unknown_cas_write_unknown,
    /// Unknown outcomes: `Server`, `Overloaded`, or another server error not
    /// mapped directly.
    unknown_server_error,
    /// Unknown outcomes: the backend's own request deadline, after the CAS
    /// was sent.
    unknown_deadline,
    /// Unknown outcomes: a dropped connection or another driver error after
    /// the CAS may have left (`Io`, `General`).
    unknown_connection,
    /// Unknown outcomes: an answer without a readable `[applied]`.
    unknown_no_applied_column,
    /// CAS errors that prove the CAS was not executed, mapped directly.
    not_applied,
    /// Serial reads (R2) run to settle an outcome or check a miss.
    serial_reads,
    /// Serial reads that themselves failed or timed out.
    serial_read_failures,
    /// Version-row reads (R3) of a superseded target version.
    version_reads,
    /// Re-runs after an unknown outcome the serial read showed unchanged.
    reruns,
    /// Requests that ended undecidable (504 or 503), re-run budget
    /// exhaustion included.
    undecidable,
    /// Applied writes whose version or index rows still failed after their
    /// retries, leaving the intent row for the sweeper (G23).
    post_cas_failures,
    /// Writes refused before any statement because what the CAS would carry
    /// is over the size bound.
    too_large,
    /// Extractions run for a new key set: one per plan built, and one per
    /// live resource a consistency check re-extracts (IW8).
    extractions,
    /// Microseconds those extractions took, summed (IW22).
    extract_us,
    /// Search parameters whose extraction failed, so the key set it returned
    /// has no rows for them (IW1, IW22). Counted per extraction, when a plan
    /// is built, before the compare-and-swap: an attempt that then misses or
    /// fails counts too, and so does each re-extraction of a consistency
    /// check.
    extraction_failures,
    /// Extractions that failed for the whole resource, so the key set they
    /// returned holds only its `resources_by_type` row (IW1, IW22, D6).
    /// Counted the same way as `extraction_failures`.
    extraction_fallbacks,
    /// Bytes of `data` and `index_keys` of every compare-and-swap attempted,
    /// counted with `attempts` just before it is sent (an attempt whose
    /// prepare failed counts too) (IW21).
    cas_payload_bytes,
}

impl fmt::Display for CasStats {
    /// `attempts=3 applied=3 …`, every counter, space-separated.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (name, value) in self.pairs() {
            if !first {
                f.write_str(" ")?;
            }
            first = false;
            write!(f, "{name}={value}")?;
        }
        Ok(())
    }
}

/// What one write's index phase packed (IW14): the index rows planned for
/// insert and for delete, the batches they were packed into, and the largest
/// of those batches by [`wire_bytes`](super::batch::wire_bytes), the measure
/// every batch is packed by (IW11). The figures are filled when the batches
/// are packed, before anything is sent, so they are not what was sent or
/// applied: a batch that failed is counted here all the same. One per write,
/// returned with its outcome, so a test or a log line reads one operation's
/// figures.
///
/// A batch here is one packed group of units, counted once however many
/// tries it took; the session's `statement_counts()`
/// ([`StatementCounts`](super::StatementCounts)) counts what was sent, the
/// requests, retries included. A group of one statement counts as a batch
/// here too, although the session sends it as a plain write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) struct IndexWriteStats {
    /// Index rows planned for insert.
    pub(crate) rows_inserted: u64,
    /// Index rows planned for delete.
    pub(crate) rows_deleted: u64,
    /// Batches the rows were packed into.
    pub(crate) batches: u64,
    /// The largest of those batches, in wire bytes; 0 with no batch.
    pub(crate) largest_batch_bytes: u64,
}

impl IndexWriteStats {
    /// Counts one batch of `inserted` inserts and `deleted` deletes taking
    /// `bytes` on the wire.
    pub(crate) fn batch(&mut self, inserted: u64, deleted: u64, bytes: u64) {
        self.rows_inserted += inserted;
        self.rows_deleted += deleted;
        self.batches += 1;
        self.largest_batch_bytes = self.largest_batch_bytes.max(bytes);
    }

    /// Every counter as `(name, value)`, in declaration order.
    pub(crate) fn pairs(&self) -> [(&'static str, u64); 4] {
        [
            ("rows_inserted", self.rows_inserted),
            ("rows_deleted", self.rows_deleted),
            ("batches", self.batches),
            ("largest_batch_bytes", self.largest_batch_bytes),
        ]
    }
}

impl fmt::Display for IndexWriteStats {
    /// `rows_inserted=1 rows_deleted=1 batches=1 largest_batch_bytes=312`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (name, value) in self.pairs() {
            if !first {
                f.write_str(" ")?;
            }
            first = false;
            write!(f, "{name}={value}")?;
        }
        Ok(())
    }
}

/// Which counter a definite miss bumps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MissKind {
    NoRow,
    Deleted,
    VersionConflict,
    AlreadyExists,
}

impl CasCounters {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn attempt(&self) {
        Self::bump(&self.attempts);
    }

    pub(crate) fn applied(&self, late: bool, superseded: bool) {
        Self::bump(&self.applied);
        if late {
            Self::bump(&self.applied_late);
        }
        if superseded {
            Self::bump(&self.applied_superseded);
        }
    }

    pub(crate) fn miss(&self, kind: MissKind) {
        Self::bump(match kind {
            MissKind::NoRow => &self.miss_no_row,
            MissKind::Deleted => &self.miss_deleted,
            MissKind::VersionConflict => &self.miss_version_conflict,
            MissKind::AlreadyExists => &self.miss_already_exists,
        });
    }

    pub(crate) fn miss_retry(&self) {
        Self::bump(&self.miss_retries);
    }

    pub(crate) fn miss_budget_exhausted(&self) {
        Self::bump(&self.miss_budget_exhausted);
    }

    /// Counts an unknown outcome under its trigger.
    pub(crate) fn unknown(&self, error: &DriverError) {
        Self::bump(match error {
            DriverError::Deadline { .. } => &self.unknown_deadline,
            // Never reached: only an index batch is refused this way, and
            // `unknown` is called only for a CAS. Counted as a server error
            // if that ever changes.
            DriverError::TooManyStatements { .. } => &self.unknown_server_error,
            DriverError::Driver(inner) => match inner.as_ref() {
                CdrsError::Server { body, .. } => match &body.ty {
                    ErrorType::WriteTimeout(timeout) => match timeout.write_type {
                        WriteType::Cas => &self.unknown_write_timeout_cas,
                        WriteType::Simple => &self.unknown_write_timeout_simple,
                        _ => &self.unknown_write_timeout_other,
                    },
                    ErrorType::WriteFailure(_) => &self.unknown_write_failure,
                    ErrorType::ReadTimeout(_) => &self.unknown_read_timeout,
                    ErrorType::ReadFailure(_) => &self.unknown_read_failure,
                    ErrorType::CasWriteUnknown(_) => &self.unknown_cas_write_unknown,
                    _ => &self.unknown_server_error,
                },
                _ => &self.unknown_connection,
            },
        });
    }

    pub(crate) fn unknown_no_applied_column(&self) {
        Self::bump(&self.unknown_no_applied_column);
    }

    pub(crate) fn not_applied(&self) {
        Self::bump(&self.not_applied);
    }

    pub(crate) fn serial_read(&self, failed: bool) {
        Self::bump(&self.serial_reads);
        if failed {
            Self::bump(&self.serial_read_failures);
        }
    }

    pub(crate) fn version_read(&self) {
        Self::bump(&self.version_reads);
    }

    pub(crate) fn rerun(&self) {
        Self::bump(&self.reruns);
    }

    pub(crate) fn undecidable(&self) {
        Self::bump(&self.undecidable);
    }

    pub(crate) fn post_cas_failure(&self) {
        Self::bump(&self.post_cas_failures);
    }

    pub(crate) fn too_large(&self) {
        Self::bump(&self.too_large);
    }

    /// One extraction for a new key set (IW1, IW22).
    pub(crate) fn extraction(&self, took: Duration, failed_params: u64, failed_resource: bool) {
        Self::bump(&self.extractions);
        self.extract_us.fetch_add(
            u64::try_from(took.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.extraction_failures
            .fetch_add(failed_params, Ordering::Relaxed);
        if failed_resource {
            Self::bump(&self.extraction_fallbacks);
        }
    }

    /// What one compare-and-swap attempt carries, `data` plus `index_keys`,
    /// counted before it is sent (IW21).
    pub(crate) fn cas_payload(&self, bytes: u64) {
        self.cas_payload_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_subtract_and_print_every_counter() {
        let counters = CasCounters::default();
        let before = counters.snapshot();
        counters.attempt();
        counters.attempt();
        counters.applied(true, false);
        counters.miss(MissKind::VersionConflict);
        counters.unknown(&DriverError::Deadline {
            after: std::time::Duration::from_secs(1),
            sent: true,
        });
        counters.extraction(Duration::from_micros(130), 2, true);
        counters.cas_payload(900);
        let delta = counters.snapshot().since(&before);
        assert_eq!(delta.attempts, 2);
        assert_eq!(
            (
                delta.extractions,
                delta.extract_us,
                delta.extraction_failures,
                delta.extraction_fallbacks,
                delta.cas_payload_bytes
            ),
            (1, 130, 2, 1, 900)
        );
        assert_eq!((delta.applied, delta.applied_late), (1, 1));
        assert_eq!(delta.miss_version_conflict, 1);
        assert_eq!(delta.unknown_deadline, 1);
        let printed = delta.to_string();
        assert!(printed.starts_with("attempts=2 applied=1 applied_late=1 "));
        assert_eq!(printed.split(' ').count(), delta.pairs().len());
    }

    #[test]
    fn index_stats_sum_rows_and_batches_and_keep_the_largest_batch() {
        let mut stats = IndexWriteStats::default();
        assert_eq!(
            stats.to_string(),
            "rows_inserted=0 rows_deleted=0 batches=0 largest_batch_bytes=0"
        );
        stats.batch(3, 2, 900);
        stats.batch(1, 0, 4_800);
        stats.batch(0, 4, 300);
        assert_eq!(
            (stats.rows_inserted, stats.rows_deleted, stats.batches),
            (4, 6, 3)
        );
        assert_eq!(stats.largest_batch_bytes, 4_800);
        assert_eq!(
            stats.to_string(),
            "rows_inserted=4 rows_deleted=6 batches=3 largest_batch_bytes=4800"
        );
    }
}
