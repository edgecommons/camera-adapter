//! Camera Adapter implementation.
//!
//! The crate implements the binding contract in `DESIGN.md` plus
//! `IMPLEMENTATION_SPEC.md`. Protocol backends are isolated behind [`backend`] traits;
//! durable jobs, admission, storage, messaging, and runtime orchestration remain protocol-neutral.

#![warn(missing_docs)]

pub mod actor;
pub mod admission;
pub mod backend;
pub mod catalog;
pub mod commands;
pub mod config;
#[cfg(any(feature = "onvif", feature = "rtsp"))]
pub mod credential_provider;
pub mod dispatch;
pub mod encoding;
pub mod error;
pub mod idempotency;
pub mod jobs;
pub mod messages;
pub mod model;
pub mod observability;
pub mod registry;
pub mod runtime;
pub mod scheduler;
pub mod state_path;
pub mod storage;
pub mod storage_pressure;
pub mod supervisor;
pub mod thumbnail;
#[cfg(windows)]
mod windows_security;

pub use error::{CameraError, ErrorCode, Result};

/// Full EdgeCommons component name used by recipes and runtime identity.
pub const COMPONENT_NAME: &str = "com.mbreissi.edgecommons.CameraAdapter";

/// Stable first-party UNS component token.
pub const COMPONENT_TOKEN: &str = "camera-adapter";
