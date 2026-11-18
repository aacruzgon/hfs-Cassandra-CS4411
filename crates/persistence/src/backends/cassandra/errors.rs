//! Mapping `cdrs-tokio` errors to the storage error types.
//!
//! The REST layer turns [`BackendError`] into an HTTP status
//! (`crates/rest/src/error.rs`, `From<BackendError>`): `Unavailable`,
//! `ConnectionFailed` and `PoolExhausted` are 503, `Timeout` is 504,
//! `UnsupportedCapability` is 501, and the rest are 500. The design's rule
//! (`cassandra-discussion.md`, "Which error, and why"; G26):
//!
//! | Driver error | `BackendError` | Status |
//! | --- | --- | --- |
//! | `Unavailable`, `Overloaded`, `IsBootstrapping` | `Unavailable` | 503 |
//! | `ReadFailure`, `WriteFailure` (replicas answered with failures) | `Unavailable` | 503 |
//! | `CdcWriteFailure` (a CDC table's log space is full; no table here uses CDC) | `Unavailable` | 503 |
//! | `ReadTimeout`, `WriteTimeout`, `CasWriteUnknown` | `Timeout` | 504 |
//! | the backend's own request deadline | `Timeout` | 504 |
//! | `Io`, `Timeout` (raised only while connecting), a closed connection, no node to send to, `Authentication`, protocol negotiation | `ConnectionFailed` | 503 |
//! | the node refused the statement (`Syntax`, `Invalid`, `Unauthorized`, `Config`, `AlreadyExists`, `FunctionFailure`) | `QueryError` | 500 |
//! | anything else (a node-side exception, a decoding error) | `Internal` | 500 |
//!
//! **Compare-and-swap outcomes must not go through this table.** It is for
//! every other statement. A CAS's failure is classified first by the CAS path
//! (ADR-aacruzgon-0024; `cassandra-discussion.md`, "When the LWT outcome is
//! unknown" and "Which error, and why"), which differs from this table in
//! three places:
//!
//! - an `Io` error, a closed connection (`General("Connection closed …")`) or
//!   the deadline after the CAS was sent leave the outcome unknown; when the
//!   `LOCAL_SERIAL` read cannot decide, the design answers **504**
//!   (`BackendError::Timeout`), where this table gives `ConnectionFailed`
//!   (503) for the first two;
//! - an error code the driver does not know (`ErrorType::Unknown`) is an
//!   unknown outcome, where this table gives `Internal` (500);
//! - a `WriteTimeout` with `write_type = SIMPLE` is an unknown outcome like
//!   any other `WriteTimeout`, confirmed by the serial read, never taken as
//!   applied (ADR-aacruzgon-0035).
//!
//! A `WriteTimeout` or `WriteFailure` with `write_type = CAS` that the serial
//! read cannot decide gets what this table gives: 504 and 503.
//!
//! Messages: `Unavailable` and `ConnectionFailed` messages are sent to the
//! client as the 503 text (`crate::error`, #2303), so they name no host; the
//! driver detail is logged instead. `Timeout`, `QueryError` and `Internal`
//! messages are logged by REST and never sent. This table's 503 texts say
//! only that Cassandra cannot serve the request or could not be reached,
//! which is true of a write that cannot have applied. Once a compare-and-swap's
//! outcome is unknown the write may have applied, so the write engine
//! (`write.rs`, `unavailable_unless_timeout` and `undecidable`) replaces the
//! message of every 503 it returns from then on with one that says the write
//! may have been stored, keeping the variant, so the status and `Retry-After`
//! stay the same (ADR-aacruzgon-0055, 0067, 0077).

use cdrs_tokio::error::Error as CdrsError;
use cdrs_tokio::frame::message_error::{ErrorType, FailureInfo};

use super::session::DriverError;
use crate::error::{BackendError, StorageError};

/// The backend name in every error and stub.
pub(crate) const BACKEND_NAME: &str = "cassandra";

/// The error every stub returns: the REST layer answers 501.
pub(crate) fn unsupported(capability: &str) -> StorageError {
    StorageError::Backend(BackendError::UnsupportedCapability {
        backend_name: BACKEND_NAME.to_string(),
        capability: capability.to_string(),
    })
}

/// `READ_TOO_MANY_TOMBSTONES` in a `ReadFailure`'s reason map: the node's
/// `RequestFailureReason` (`cassandra-5.0.9`, `READ_TOO_MANY_TOMBSTONES (1)`),
/// which native protocol v5 sends per replica.
const READ_TOO_MANY_TOMBSTONES: i16 = 1;

/// Whether a replica refused this read for reading more than
/// `tombstone_failure_threshold` tombstones (`ReadCommand.java:590-602` in
/// `cassandra-5.0.9`). A single replica is enough: its read cannot succeed
/// until the tombstones become purgeable.
pub(crate) fn is_too_many_tombstones(error: &DriverError) -> bool {
    let DriverError::Driver(error) = error else {
        return false;
    };
    let CdrsError::Server { body, .. } = error.as_ref() else {
        return false;
    };
    let ErrorType::ReadFailure(failure) = &body.ty else {
        return false;
    };
    matches!(
        &failure.failure_info,
        FailureInfo::ReasonMap(reasons)
            if reasons.values().any(|reason| *reason == READ_TOO_MANY_TOMBSTONES)
    )
}

/// Maps a failed statement to a [`BackendError`]. `operation` names what was
/// being done, for the log and for the messages that are not sent to clients.
pub(crate) fn map_driver_error(error: &DriverError, operation: &str) -> BackendError {
    let mapped = classify(error, operation);
    match &mapped {
        BackendError::Unavailable { .. } | BackendError::ConnectionFailed { .. } => {
            tracing::warn!(operation, error = %describe(error), "Cassandra unavailable");
        }
        BackendError::Timeout { .. } => {
            tracing::warn!(operation, error = %describe(error), "Cassandra timed out");
        }
        _ => {
            tracing::error!(operation, error = %describe(error), "Cassandra statement failed");
        }
    }
    mapped
}

fn classify(error: &DriverError, operation: &str) -> BackendError {
    let error = match error {
        DriverError::Deadline { after, .. } => {
            return timeout(format!(
                "{operation} exceeded the backend's request deadline of {} ms",
                after.as_millis()
            ));
        }
        DriverError::Driver(error) => error,
    };

    match error.as_ref() {
        CdrsError::Server { body, .. } => match &body.ty {
            ErrorType::Unavailable(_) | ErrorType::Overloaded | ErrorType::IsBootstrapping => {
                unavailable()
            }
            ErrorType::ReadFailure(_) | ErrorType::WriteFailure(_) | ErrorType::CdcWriteFailure => {
                unavailable()
            }
            // `CasWriteUnknown` reaches this table only for a statement that
            // is not a CAS, which no node sends; it is a timeout like the rest.
            ErrorType::ReadTimeout(_)
            | ErrorType::WriteTimeout(_)
            | ErrorType::CasWriteUnknown(_) => timeout(format!("{operation}: {}", body.message)),
            ErrorType::Authentication => connection_failed(),
            ErrorType::Syntax
            | ErrorType::Invalid
            | ErrorType::Unauthorized
            | ErrorType::Config
            | ErrorType::AlreadyExists(_)
            | ErrorType::FunctionFailure(_) => BackendError::QueryError {
                message: format!("{operation}: {}", body.message),
            },
            // `Server`, `Protocol`, `Truncate`, `Unprepared` (the driver
            // already re-prepared and re-sent it, up to 5 times,
            // `cluster/session.rs:65-70`, `:331-377`), `Unknown` (a code the driver cannot
            // name), and any variant a later driver adds (`ErrorType` is
            // `#[non_exhaustive]`).
            _ => internal(format!("{operation}: {}", body.message)),
        },
        CdrsError::Io(_) | CdrsError::Timeout(_) | CdrsError::InvalidProtocol(_) => {
            connection_failed()
        }
        CdrsError::General(message) if is_connection_message(message) => connection_failed(),
        // Decoding errors, and any variant a later driver adds (`Error` is
        // `#[non_exhaustive]`).
        other => internal(format!("{operation}: {other}")),
    }
}

/// `General` messages `cdrs-tokio` 10.0.0 raises when no connection could
/// carry the request: `transport.rs` ("Connection closed when writing data!",
/// "Connection closed while waiting for response!", `:487`, `:491`), the
/// session ("No nodes available in query plan!", `cluster/session.rs:892`,
/// `:906`, only for an empty plan), the connection pool ("No active
/// connections to: …", `cluster/connection_pool.rs:711-716`, while a node is
/// still marked up), the write channel ("Write channel failure!",
/// `transport.rs:684`, `:769`), and session construction ("Session control
/// connection died before completing initialization", `cluster/session.rs:1167`).
fn is_connection_message(message: &str) -> bool {
    message.contains("Connection closed")
        || message.contains("No nodes available")
        || message.contains("No active connections")
        || message.contains("Write channel failure")
        || message.contains("control connection")
}

/// The driver detail, without the node address `Server` errors carry in
/// their `Display`.
fn describe(error: &DriverError) -> String {
    match error {
        DriverError::Deadline { after, sent } => format!(
            "request deadline of {} ms ({})",
            after.as_millis(),
            if *sent {
                "after sending"
            } else {
                "before sending"
            }
        ),
        DriverError::Driver(error) => match error.as_ref() {
            CdrsError::Server { body, addr } => {
                format!("{:?} from {addr}: {}", body.ty, body.message)
            }
            other => other.to_string(),
        },
    }
}

fn unavailable() -> BackendError {
    BackendError::Unavailable {
        backend_name: BACKEND_NAME.to_string(),
        message: "Cassandra cannot serve the request right now".to_string(),
    }
}

fn connection_failed() -> BackendError {
    BackendError::ConnectionFailed {
        backend_name: BACKEND_NAME.to_string(),
        message: "no Cassandra node could be reached".to_string(),
    }
}

fn timeout(message: String) -> BackendError {
    BackendError::Timeout {
        backend_name: BACKEND_NAME.to_string(),
        message,
    }
}

fn internal(message: String) -> BackendError {
    BackendError::Internal {
        backend_name: BACKEND_NAME.to_string(),
        message,
        source: None,
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use cdrs_tokio::consistency::Consistency;
    use cdrs_tokio::frame::Version;
    use cdrs_tokio::frame::message_error::{
        CasWriteUnknownError, ErrorBody, FailureInfo, ReadFailureError, UnavailableError,
        WriteFailureError, WriteTimeoutError, WriteType,
    };
    use std::io::Cursor;
    use std::net::SocketAddr;
    use std::time::Duration;

    fn server(ty: ErrorType) -> DriverError {
        DriverError::from(CdrsError::Server {
            body: ErrorBody::new("node said no", ty),
            addr: "192.168.147.2:9042".parse::<SocketAddr>().unwrap(),
        })
    }

    fn read_failure(failure_info: FailureInfo) -> DriverError {
        server(ErrorType::ReadFailure(ReadFailureError::new(
            Consistency::LocalQuorum,
            0,
            1,
            failure_info,
            false,
        )))
    }

    fn reasons(codes: &[i16]) -> FailureInfo {
        FailureInfo::ReasonMap(
            codes
                .iter()
                .enumerate()
                .map(|(n, code)| (std::net::IpAddr::from([10, 0, 0, n as u8 + 1]), *code))
                .collect(),
        )
    }

    /// `TIMEOUT (2)`, `READ_SIZE (4)` and the rest are not this failure, and
    /// protocol v4's bare count names no reason at all.
    #[test]
    fn only_the_tombstone_reason_counts_as_too_many_tombstones() {
        assert!(is_too_many_tombstones(&read_failure(reasons(&[1]))));
        assert!(
            is_too_many_tombstones(&read_failure(reasons(&[2, 1]))),
            "one replica of a quorum is enough"
        );
        assert!(!is_too_many_tombstones(&read_failure(reasons(&[0, 2, 4]))));
        assert!(!is_too_many_tombstones(&read_failure(
            FailureInfo::NumFailures(1)
        )));
        assert!(!is_too_many_tombstones(&server(write_failure(
            WriteType::Simple
        ))));
    }

    fn write_timeout(write_type: WriteType) -> ErrorType {
        ErrorType::WriteTimeout(WriteTimeoutError::new(
            Consistency::LocalQuorum,
            0,
            1,
            write_type,
            None,
        ))
    }

    fn write_failure(write_type: WriteType) -> ErrorType {
        ErrorType::WriteFailure(WriteFailureError::new(
            Consistency::LocalQuorum,
            0,
            1,
            FailureInfo::NumFailures(1),
            write_type,
        ))
    }

    fn status_family(error: &BackendError) -> &'static str {
        match error {
            BackendError::Unavailable { .. }
            | BackendError::ConnectionFailed { .. }
            | BackendError::PoolExhausted { .. } => "503",
            BackendError::Timeout { .. } => "504",
            BackendError::UnsupportedCapability { .. } => "501",
            _ => "500",
        }
    }

    #[test]
    fn unavailable_and_replica_failures_are_503() {
        let unavailable =
            ErrorType::Unavailable(UnavailableError::new(Consistency::LocalQuorum, 2, 1));
        for ty in [
            unavailable,
            ErrorType::Overloaded,
            ErrorType::IsBootstrapping,
            ErrorType::CdcWriteFailure,
            write_failure(WriteType::Simple),
            write_failure(WriteType::Cas),
        ] {
            let mapped = map_driver_error(&server(ty.clone()), "test");
            assert!(
                matches!(mapped, BackendError::Unavailable { .. }),
                "{ty:?} -> {mapped:?}"
            );
        }
    }

    #[test]
    fn timeouts_are_504_including_the_backend_deadline() {
        for error in [
            server(write_timeout(WriteType::Simple)),
            server(write_timeout(WriteType::Cas)),
            server(write_timeout(WriteType::Batch)),
            server(ErrorType::CasWriteUnknown(CasWriteUnknownError::new(
                Consistency::LocalSerial,
                0,
                1,
            ))),
            DriverError::Deadline {
                after: Duration::from_millis(10_000),
                sent: true,
            },
            DriverError::Deadline {
                after: Duration::from_millis(10_000),
                sent: false,
            },
        ] {
            let mapped = map_driver_error(&error, "test");
            assert_eq!(status_family(&mapped), "504", "{error:?} -> {mapped:?}");
        }
    }

    #[test]
    fn transport_failures_are_connection_failed() {
        for error in [
            CdrsError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "refused",
            )),
            CdrsError::Timeout("connect".to_string()),
            CdrsError::General("Connection closed when writing data!".to_string()),
            CdrsError::General("Connection closed while waiting for response!".to_string()),
            CdrsError::General("No nodes available in query plan!".to_string()),
            CdrsError::General("No active connections to: 10.0.0.1:9042".to_string()),
            CdrsError::General("Write channel failure!".to_string()),
        ] {
            let mapped = map_driver_error(&DriverError::from(error), "test");
            assert!(
                matches!(mapped, BackendError::ConnectionFailed { .. }),
                "{mapped:?}"
            );
        }
    }

    /// An error code the driver does not recognize, decoded the way the driver
    /// decodes an ERROR body: `0x2600` is not in the v5 table, so
    /// `from_cursor_with_code` keeps it as `Unknown` with its trailing bytes.
    pub(in crate::backends::cassandra) fn undecoded_error_type() -> ErrorType {
        let trailing: &[u8] = &[1, 2, 3];
        let ty = ErrorType::from_cursor_with_code(&mut Cursor::new(trailing), 0x2600, Version::V5)
            .unwrap();
        assert_eq!(
            ty,
            ErrorType::Unknown {
                code: 0x2600,
                body: vec![1, 2, 3],
            }
        );
        ty
    }

    #[test]
    fn refused_statements_are_query_errors() {
        for ty in [ErrorType::Syntax, ErrorType::Invalid] {
            let mapped = map_driver_error(&server(ty.clone()), "test");
            assert!(
                matches!(mapped, BackendError::QueryError { .. }),
                "{ty:?} -> {mapped:?}"
            );
        }
    }

    #[test]
    fn server_and_unrecognized_errors_are_internal() {
        for ty in [ErrorType::Server, undecoded_error_type()] {
            let mapped = map_driver_error(&server(ty.clone()), "test");
            assert!(
                matches!(mapped, BackendError::Internal { .. }),
                "{ty:?} -> {mapped:?}"
            );
        }
    }

    #[test]
    fn client_facing_messages_name_no_host() {
        let unavailable = map_driver_error(&server(write_failure(WriteType::Simple)), "test");
        let connection = map_driver_error(
            &DriverError::from(CdrsError::Io(std::io::Error::other("10.1.2.3:9042"))),
            "test",
        );
        for error in [unavailable, connection] {
            let text = error.to_string();
            assert!(!text.contains("192.168.147.2"), "{text}");
            assert!(!text.contains("10.1.2.3"), "{text}");
        }
    }

    #[test]
    fn stubs_are_unsupported_capability_named_cassandra() {
        match unsupported("history") {
            StorageError::Backend(BackendError::UnsupportedCapability {
                backend_name,
                capability,
            }) => {
                assert_eq!(backend_name, "cassandra");
                assert_eq!(capability, "history");
            }
            other => panic!("{other:?}"),
        }
    }
}
