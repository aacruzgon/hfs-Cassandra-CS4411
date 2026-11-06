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
//! | `ReadTimeout`, `WriteTimeout` | `Timeout` | 504 |
//! | the backend's own request deadline | `Timeout` | 504 |
//! | `Io`, `Timeout` (raised only while connecting), a closed connection, no node to send to, `Authentication`, protocol negotiation | `ConnectionFailed` | 503 |
//! | the node refused the statement (`Syntax`, `Invalid`, `Unauthorized`, `Config`, `AlreadyExists`, `FunctionFailure`) | `QueryError` | 500 |
//! | anything else (a node-side exception, a decoding error) | `Internal` | 500 |
//!
//! **Compare-and-swap outcomes are not decided here.** A `WriteTimeout` or
//! `WriteFailure` with `write_type = CAS`, a closed connection or the deadline
//! around a CAS leave its outcome unknown, and Step 3 resolves them with a
//! `LOCAL_SERIAL` read before anything is mapped (ADR-aacruzgon-0024). When
//! that read cannot decide, the design returns exactly what this table gives:
//! 504 after a timeout or a dropped connection's deadline, 503 after a
//! failure. A `WriteTimeout` with `write_type = SIMPLE` on a CAS means it
//! applied; that too is the CAS path's call, not this table's.
//!
//! Messages: `Unavailable` and `ConnectionFailed` messages are sent to the
//! client as the 503 text (`crate::error`, #2303), so they name no host; the
//! driver detail is logged instead. `Timeout`, `QueryError` and `Internal`
//! messages are logged by REST and never sent.

use cdrs_tokio::error::Error as CdrsError;
use cdrs_tokio::frame::message_error::ErrorType;

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
        DriverError::Deadline(after) => {
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
            ErrorType::ReadFailure(_) | ErrorType::WriteFailure(_) => unavailable(),
            ErrorType::ReadTimeout(_) | ErrorType::WriteTimeout(_) => {
                timeout(format!("{operation}: {}", body.message))
            }
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
            // already re-prepared once), and any code a later protocol adds.
            _ => internal(format!("{operation}: {}", body.message)),
        },
        CdrsError::Io(_) | CdrsError::Timeout(_) | CdrsError::InvalidProtocol(_) => {
            connection_failed()
        }
        CdrsError::General(message) if is_connection_message(message) => connection_failed(),
        other => internal(format!("{operation}: {other}")),
    }
}

/// `General` messages the driver raises when no connection could carry the
/// request: `transport.rs` ("Connection closed when writing data!",
/// "Connection closed while waiting for response!"), the session ("No nodes
/// available in query plan!"), and session construction ("Session control
/// connection died before completing initialization").
fn is_connection_message(message: &str) -> bool {
    message.contains("Connection closed")
        || message.contains("No nodes available")
        || message.contains("control connection")
}

/// The driver detail, without the node address `Server` errors carry in
/// their `Display`.
fn describe(error: &DriverError) -> String {
    match error {
        DriverError::Deadline(after) => format!("request deadline of {} ms", after.as_millis()),
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
mod tests {
    use super::*;
    use cdrs_tokio::consistency::Consistency;
    use cdrs_tokio::frame::message_error::{
        ErrorBody, FailureInfo, UnavailableError, WriteFailureError, WriteTimeoutError, WriteType,
    };
    use std::net::SocketAddr;
    use std::time::Duration;

    fn server(ty: ErrorType) -> DriverError {
        DriverError::from(CdrsError::Server {
            body: ErrorBody {
                message: "node said no".to_string(),
                ty,
            },
            addr: "192.168.147.2:9042".parse::<SocketAddr>().unwrap(),
        })
    }

    fn write_timeout(write_type: WriteType) -> ErrorType {
        ErrorType::WriteTimeout(WriteTimeoutError {
            cl: Consistency::LocalQuorum,
            received: 0,
            block_for: 1,
            contentions: None,
            write_type,
        })
    }

    fn write_failure(write_type: WriteType) -> ErrorType {
        ErrorType::WriteFailure(WriteFailureError {
            cl: Consistency::LocalQuorum,
            received: 0,
            block_for: 1,
            failure_info: FailureInfo::NumFailures(1),
            write_type,
        })
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
        let unavailable = ErrorType::Unavailable(UnavailableError {
            cl: Consistency::LocalQuorum,
            required: 2,
            alive: 1,
        });
        for ty in [
            unavailable,
            ErrorType::Overloaded,
            ErrorType::IsBootstrapping,
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
            DriverError::Deadline(Duration::from_millis(10_000)),
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
        ] {
            let mapped = map_driver_error(&DriverError::from(error), "test");
            assert!(
                matches!(mapped, BackendError::ConnectionFailed { .. }),
                "{mapped:?}"
            );
        }
    }

    #[test]
    fn refused_statements_are_500() {
        for ty in [ErrorType::Syntax, ErrorType::Invalid, ErrorType::Server] {
            let mapped = map_driver_error(&server(ty.clone()), "test");
            assert_eq!(status_family(&mapped), "500", "{ty:?} -> {mapped:?}");
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
