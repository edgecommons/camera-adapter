//! Typed camera-adapter errors and stable public error codes.

use std::borrow::Cow;

use thiserror::Error;

/// Crate-wide result type.
pub type Result<T> = std::result::Result<T, CameraError>;

/// Stable command and terminal-failure codes from the camera messaging contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCode {
    /// A target is required because more than one camera is configured.
    InstanceRequired,
    /// The requested camera is not configured.
    UnknownInstance,
    /// The requested camera is disabled.
    CameraDisabled,
    /// The camera cannot currently serve the request.
    CameraUnavailable,
    /// The capture/PTZ interlock rejected the operation.
    CameraMoving,
    /// The backend or camera lacks the requested capability.
    UnsupportedCapability,
    /// The request body or a value is invalid.
    InvalidRequest,
    /// The named capture profile is not configured.
    UnknownCaptureProfile,
    /// A bounded camera/control queue is full.
    QueueFull,
    /// A group contains too many cameras.
    GroupTooLarge,
    /// A memory, disk, encoder, writer, or resource-group bound rejected work.
    ResourceLimit,
    /// The capture or one of its stages exceeded a deadline.
    CaptureTimeout,
    /// The capture reached the cancelled terminal state.
    CaptureCancelled,
    /// A restart interrupted non-resumable work.
    ProcessInterrupted,
    /// The durable job or group does not exist or has expired.
    CaptureNotFound,
    /// A reused request identifier has different immutable arguments.
    IdempotencyConflict,
    /// Prior physical actuation may have happened before a crash.
    PreviousOutcomeUnknown,
    /// A terminal command requires request/reply metadata.
    ReplyRequired,
    /// The source frame cannot produce the configured output.
    UnsupportedPixelFormat,
    /// Free-space or reserved-capacity policy rejected work.
    StoragePressure,
    /// File persistence finalization or verification failed.
    PersistenceFailed,
    /// PTZ is disabled for the selected camera.
    PtzDisabled,
    /// A normalized PTZ coordinate is outside its permitted range.
    PtzRangeError,
    /// A PTZ protocol operation exceeded its deadline.
    PtzTimeout,
    /// Shutdown has begun and new work is rejected.
    ComponentStopping,
    /// A protocol backend returned a safely summarized failure.
    BackendError,
}

impl ErrorCode {
    /// Returns the exact SCREAMING_SNAKE_CASE wire value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InstanceRequired => "INSTANCE_REQUIRED",
            Self::UnknownInstance => "UNKNOWN_INSTANCE",
            Self::CameraDisabled => "CAMERA_DISABLED",
            Self::CameraUnavailable => "CAMERA_UNAVAILABLE",
            Self::CameraMoving => "CAMERA_MOVING",
            Self::UnsupportedCapability => "UNSUPPORTED_CAPABILITY",
            Self::InvalidRequest => "INVALID_REQUEST",
            Self::UnknownCaptureProfile => "UNKNOWN_CAPTURE_PROFILE",
            Self::QueueFull => "QUEUE_FULL",
            Self::GroupTooLarge => "GROUP_TOO_LARGE",
            Self::ResourceLimit => "RESOURCE_LIMIT",
            Self::CaptureTimeout => "CAPTURE_TIMEOUT",
            Self::CaptureCancelled => "CAPTURE_CANCELLED",
            Self::ProcessInterrupted => "PROCESS_INTERRUPTED",
            Self::CaptureNotFound => "CAPTURE_NOT_FOUND",
            Self::IdempotencyConflict => "IDEMPOTENCY_CONFLICT",
            Self::PreviousOutcomeUnknown => "PREVIOUS_OUTCOME_UNKNOWN",
            Self::ReplyRequired => "REPLY_REQUIRED",
            Self::UnsupportedPixelFormat => "UNSUPPORTED_PIXEL_FORMAT",
            Self::StoragePressure => "STORAGE_PRESSURE",
            Self::PersistenceFailed => "PERSISTENCE_FAILED",
            Self::PtzDisabled => "PTZ_DISABLED",
            Self::PtzRangeError => "PTZ_RANGE_ERROR",
            Self::PtzTimeout => "PTZ_TIMEOUT",
            Self::ComponentStopping => "COMPONENT_STOPPING",
            Self::BackendError => "BACKEND_ERROR",
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Typed adapter failure with a stable public category and an operator-safe detail.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CameraError {
    /// Closed-schema or cross-field configuration failure.
    #[error("configuration error at {path}: {message}")]
    Config {
        /// JSON-style path to the rejected field.
        path: String,
        /// Operator-safe explanation.
        message: String,
    },
    /// Stable command/domain rejection.
    #[error("{code}: {message}")]
    Rejected {
        /// Public error code.
        code: ErrorCode,
        /// Operator-safe explanation.
        message: Cow<'static, str>,
    },
    /// Protocol backend failure.
    #[error("backend '{backend}' failed: {message}")]
    Backend {
        /// Stable backend kind.
        backend: &'static str,
        /// Sanitized detail without credentials or unsafe URIs.
        message: String,
    },
    /// Durable catalog failure.
    #[error("catalog error: {0}")]
    Catalog(String),
    /// Guarded messaging/envelope preparation failure.
    #[error("messaging error: {0}")]
    Messaging(String),
    /// Storage/path/persistence failure.
    #[error("storage error: {0}")]
    Storage(String),
    /// Underlying filesystem error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Underlying JSON error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// Underlying SQLite error.
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

impl CameraError {
    /// Builds a public rejection without allocating a static message.
    #[must_use]
    pub fn rejected(code: ErrorCode, message: impl Into<Cow<'static, str>>) -> Self {
        Self::Rejected {
            code,
            message: message.into(),
        }
    }

    /// The detail that may leave the component: in a terminal message on the bus, or in a command
    /// reply.
    ///
    /// `Display` is for logs, and it is not the same thing. Three variants wrap a foreign error whose
    /// `Display` is written for a developer reading a stack trace -- and this string is broadcast to
    /// every subscriber on the UNS bus. `SQLite error: UNIQUE constraint failed: jobs.capture_id` told
    /// the whole fleet the adapter's table and column names, and the doc for the field it lands in
    /// (`FailureSummary.message`) has always said "sanitized operator-safe detail".
    ///
    /// Everything else here is a string this codebase wrote on purpose, for an operator to read, and
    /// passes through unchanged.
    #[must_use]
    pub fn operator_detail(&self) -> Cow<'static, str> {
        match self {
            Self::Rejected { message, .. } => message.clone(),
            Self::Config { path, message } => {
                Cow::Owned(format!("configuration error at {path}: {message}"))
            }
            Self::Backend { backend, message } => {
                Cow::Owned(format!("backend '{backend}' failed: {message}"))
            }
            Self::Catalog(message) | Self::Messaging(message) | Self::Storage(message) => {
                Cow::Owned(message.clone())
            }
            // The three that wrap somebody else's error type. An operator gets to know WHICH part of
            // the component failed -- which is all they can act on anyway -- and no more.
            Self::Io(_) => Cow::Borrowed("the output filesystem reported an error"),
            Self::Json(_) => Cow::Borrowed("a request or record could not be parsed"),
            Self::Sqlite(_) => Cow::Borrowed("the durable store reported an error"),
        }
    }

    /// Returns a stable public code when this error is command-visible.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::Rejected { code, .. } => *code,
            Self::Storage(_) | Self::Io(_) => ErrorCode::PersistenceFailed,
            Self::Backend { .. } => ErrorCode::BackendError,
            Self::Config { .. } | Self::Json(_) => ErrorCode::InvalidRequest,
            Self::Catalog(_) | Self::Messaging(_) | Self::Sqlite(_) => ErrorCode::BackendError,
        }
    }

    /// Whether this failure came from the durable store rather than from the camera.
    ///
    /// The distinction decides whether a camera survives a bad moment. A capture that fails is
    /// recorded and reported; an error that escapes the job engine is the engine saying it could not
    /// run the job at all, and the actor used to read every one of those as proof that the protocol
    /// session was dead -- stop accepting, drain the queue, close the session, hand the supervisor a
    /// failure, get reconnected.
    ///
    /// SQLite is not the camera. A `SQLITE_BUSY` from a contended connection pool, a full disk, an
    /// I/O hiccup: none of them are evidence that the camera stopped answering. Disconnecting on
    /// them turns a slow disk into a fleet-wide reconnect storm -- and the storm makes the
    /// contention worse, because every reconnect writes.
    ///
    /// Deliberately narrow. It covers the two variants that can only be the store misbehaving, and
    /// nothing else:
    ///
    /// - `Catalog` is NOT included. It carries durable *invariant violations* as well as store
    ///   errors -- "capture actor expected QUEUED, found Acquiring" means another writer advanced
    ///   the record underneath this one, and the design deliberately retires the actor and starts a
    ///   fresh generation rather than let a dispatcher sit wedged behind a session it no longer
    ///   agrees with. That is a real fatality, not a hiccup.
    /// - `Io` is NOT included, because a protocol backend could surface a socket error through it,
    ///   and wrongly keeping a dead session is worse than wrongly dropping a live one.
    #[must_use]
    pub const fn is_durable_store_failure(&self) -> bool {
        matches!(self, Self::Sqlite(_) | Self::Storage(_))
    }
}

#[cfg(test)]
mod tests {

    /// What an operator is told is not what a developer is told.
    ///
    /// `FailureSummary.message` is documented as "sanitized operator-safe detail" and is broadcast to
    /// every subscriber on the UNS bus. It carried the raw `Display` of whatever went wrong -- so
    /// `SQLite error: UNIQUE constraint failed: jobs.capture_id` told the whole fleet the adapter's
    /// table and column names. No credential, but no business of theirs either, and not what the field
    /// says it holds.
    #[test]
    fn the_detail_that_leaves_the_component_never_carries_a_foreign_error() {
        let sqlite = CameraError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(2067),
            Some("UNIQUE constraint failed: jobs.capture_id".to_owned()),
        ));
        let detail = sqlite.operator_detail();
        assert!(
            !detail.contains("jobs.capture_id") && !detail.contains("UNIQUE"),
            "the durable store's schema is not the fleet's business: {detail}"
        );
        assert!(
            sqlite.to_string().contains("jobs.capture_id"),
            "the log still gets the whole truth -- that is what Display is for"
        );

        let io = CameraError::Io(std::io::Error::other("/srv/secret/path denied"));
        assert!(!io.operator_detail().contains("/srv/secret/path"));

        // A message this codebase wrote for an operator to read passes through untouched.
        let rejected = CameraError::rejected(ErrorCode::QueueFull, "the camera queue is full");
        assert_eq!(rejected.operator_detail(), "the camera queue is full");
        let backend = CameraError::Backend {
            backend: "onvif",
            message: "the camera refused the media profile".to_owned(),
        };
        assert!(
            backend
                .operator_detail()
                .contains("refused the media profile")
        );
    }
    use super::*;

    /// A busy disk is not a broken camera.
    ///
    /// The actor tears the camera session down -- stop accepting, drain, close, hand the supervisor a
    /// failure, get reconnected -- for any error that escapes the job engine. Under contention the
    /// catalog's two connections return `SQLITE_BUSY`, which arrives here as `Sqlite`, and that used
    /// to disconnect the camera. At fleet scale it cascaded: each reconnect writes, the writes
    /// deepen the contention, and the contention disconnects more cameras.
    ///
    /// This predicate is the whole decision, so it is worth pinning both directions of it -- and the
    /// negative cases are the load-bearing ones.
    #[test]
    fn only_the_store_misbehaving_spares_the_camera_session() {
        assert!(
            CameraError::Sqlite(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(5), // SQLITE_BUSY
                Some("database is locked".to_owned()),
            ))
            .is_durable_store_failure(),
            "a contended connection pool must not disconnect a camera"
        );
        assert!(
            CameraError::Storage("no space left on device".to_owned()).is_durable_store_failure(),
            "a full disk must not disconnect a camera"
        );

        assert!(
            !CameraError::Catalog("capture actor expected QUEUED, found Acquiring".to_owned())
                .is_durable_store_failure(),
            "a durable invariant violation is a real fatality: another writer advanced the record, \
             and the actor must be retired rather than left wedged behind it"
        );
        assert!(
            !CameraError::Backend {
                backend: "onvif-rtsp",
                message: "session closed".to_owned(),
            }
            .is_durable_store_failure(),
            "the camera itself failing must still end the session"
        );
        assert!(
            !CameraError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset))
                .is_durable_store_failure(),
            "a backend can surface a socket error as Io; keeping a dead session is the worse mistake"
        );
    }

    /// A parse failure names the component's own internals; an operator gets told none of them.
    ///
    /// `serde_json`'s `Display` is written for a developer holding the document -- "expected value at
    /// line 3 column 17", "unknown field `sha256`" -- and `FailureSummary.message` is broadcast to
    /// every subscriber on the UNS bus. `Json` is the variant a malformed recovery record and a
    /// malformed request both arrive as, so it is the one most likely to be carrying the shape of a
    /// durable row when it lands on the wire.
    ///
    /// The counterpart is the other half of the contract, and it is the reason `operator_detail` is a
    /// match and not a blanket redaction: a `Config` error is a sentence this codebase wrote FOR an
    /// operator, naming the configuration path they must go and fix. Redacting that would leave them
    /// with a component that refuses to start and no way to learn why.
    #[test]
    fn a_foreign_parse_error_is_generic_while_an_authored_configuration_detail_is_not() {
        let json = CameraError::Json(
            serde_json::from_str::<serde_json::Value>(r#"{"expectedSha256": }"#)
                .expect_err("intentionally malformed JSON"),
        );
        let detail = json.operator_detail();
        assert_eq!(detail, "a request or record could not be parsed");
        assert!(
            !detail.contains("column") && !detail.contains("expectedSha256"),
            "serde's developer-facing detail must not be broadcast to the fleet: {detail}"
        );
        assert!(
            json.to_string().starts_with("JSON error:"),
            "the log still gets the whole truth -- that is what Display is for"
        );

        let config = CameraError::Config {
            path: "component.instances[0].backend".to_owned(),
            message: "mediaProfile is required".to_owned(),
        };
        assert_eq!(
            config.operator_detail(),
            "configuration error at component.instances[0].backend: mediaProfile is required",
            "an operator who must go and fix a configuration path has to be told which one"
        );
    }

    #[test]
    fn public_codes_and_internal_error_categories_map_stably() {
        assert_eq!(
            ErrorCode::ProcessInterrupted.to_string(),
            "PROCESS_INTERRUPTED"
        );
        assert_eq!(
            CameraError::rejected(ErrorCode::CameraDisabled, "disabled").code(),
            ErrorCode::CameraDisabled
        );
        assert_eq!(
            CameraError::Config {
                path: "x".to_string(),
                message: "bad".to_string()
            }
            .code(),
            ErrorCode::InvalidRequest
        );
        assert_eq!(
            CameraError::Storage("disk".to_string()).code(),
            ErrorCode::PersistenceFailed
        );
        assert_eq!(
            CameraError::Catalog("busy".to_string()).code(),
            ErrorCode::BackendError
        );
    }

    #[test]
    fn every_public_error_code_has_its_contract_wire_spelling() {
        let cases = [
            (ErrorCode::InstanceRequired, "INSTANCE_REQUIRED"),
            (ErrorCode::UnknownInstance, "UNKNOWN_INSTANCE"),
            (ErrorCode::CameraDisabled, "CAMERA_DISABLED"),
            (ErrorCode::CameraUnavailable, "CAMERA_UNAVAILABLE"),
            (ErrorCode::CameraMoving, "CAMERA_MOVING"),
            (ErrorCode::UnsupportedCapability, "UNSUPPORTED_CAPABILITY"),
            (ErrorCode::InvalidRequest, "INVALID_REQUEST"),
            (ErrorCode::UnknownCaptureProfile, "UNKNOWN_CAPTURE_PROFILE"),
            (ErrorCode::QueueFull, "QUEUE_FULL"),
            (ErrorCode::GroupTooLarge, "GROUP_TOO_LARGE"),
            (ErrorCode::ResourceLimit, "RESOURCE_LIMIT"),
            (ErrorCode::CaptureTimeout, "CAPTURE_TIMEOUT"),
            (ErrorCode::CaptureCancelled, "CAPTURE_CANCELLED"),
            (ErrorCode::ProcessInterrupted, "PROCESS_INTERRUPTED"),
            (ErrorCode::CaptureNotFound, "CAPTURE_NOT_FOUND"),
            (ErrorCode::IdempotencyConflict, "IDEMPOTENCY_CONFLICT"),
            (
                ErrorCode::PreviousOutcomeUnknown,
                "PREVIOUS_OUTCOME_UNKNOWN",
            ),
            (ErrorCode::ReplyRequired, "REPLY_REQUIRED"),
            (
                ErrorCode::UnsupportedPixelFormat,
                "UNSUPPORTED_PIXEL_FORMAT",
            ),
            (ErrorCode::StoragePressure, "STORAGE_PRESSURE"),
            (ErrorCode::PersistenceFailed, "PERSISTENCE_FAILED"),
            (ErrorCode::PtzDisabled, "PTZ_DISABLED"),
            (ErrorCode::PtzRangeError, "PTZ_RANGE_ERROR"),
            (ErrorCode::PtzTimeout, "PTZ_TIMEOUT"),
            (ErrorCode::ComponentStopping, "COMPONENT_STOPPING"),
            (ErrorCode::BackendError, "BACKEND_ERROR"),
        ];

        for (code, wire_value) in cases {
            assert_eq!(code.as_str(), wire_value);
            assert_eq!(code.to_string(), wire_value);
        }
    }

    #[test]
    fn error_categories_preserve_public_codes_and_safe_display_context() {
        let json_error = serde_json::from_str::<serde_json::Value>("{")
            .expect_err("intentionally malformed JSON");
        let errors = [
            (
                CameraError::Backend {
                    backend: "sim",
                    message: "bounded protocol detail".to_owned(),
                },
                ErrorCode::BackendError,
                "backend 'sim' failed",
            ),
            (
                CameraError::Messaging("reply metadata missing".to_owned()),
                ErrorCode::BackendError,
                "messaging error",
            ),
            (
                CameraError::Io(std::io::Error::other("durability failure")),
                ErrorCode::PersistenceFailed,
                "I/O error",
            ),
            (
                CameraError::Json(json_error),
                ErrorCode::InvalidRequest,
                "JSON error",
            ),
            (
                CameraError::Sqlite(rusqlite::Error::InvalidQuery),
                ErrorCode::BackendError,
                "SQLite error",
            ),
        ];

        for (error, code, display_prefix) in errors {
            assert_eq!(error.code(), code);
            assert!(error.to_string().starts_with(display_prefix));
        }

        let dynamic = CameraError::rejected(
            ErrorCode::ResourceLimit,
            String::from("frame allocation exceeds configured maximum"),
        );
        assert_eq!(dynamic.code(), ErrorCode::ResourceLimit);
        assert!(
            dynamic
                .to_string()
                .contains("frame allocation exceeds configured maximum")
        );
    }
}
