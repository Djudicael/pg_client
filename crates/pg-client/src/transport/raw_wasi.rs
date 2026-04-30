//! Fallback raw WASI transport.
//!
//! This module is a stub for a direct `wasi:sockets/tcp` implementation without
//! the `wstd` wrapper. It is not compiled by default — `WasiTcpTransport` in
//! `tcp.rs` is the preferred implementation.
//!
//! If `wstd` proves insufficient (API bugs, missing features, version
//! incompatibility), this module can be fleshed out as a drop-in replacement.

#![allow(dead_code)]

use super::TransportError;

/// Raw WASI transport (stub — not implemented).
pub struct RawWasiTransport;

impl RawWasiTransport {
    pub async fn connect(_host: &str, _port: u16) -> Result<Self, TransportError> {
        Err(TransportError::Unsupported(
            "RawWasiTransport is not implemented. Use WasiTcpTransport instead.".into(),
        ))
    }
}
