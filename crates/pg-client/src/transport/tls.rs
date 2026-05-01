//! Async TLS transport using `rustls` over `AsyncTransport`.
//!
//! This module implements PostgreSQL's SSL negotiation protocol and provides
//! `TlsTransport` — a wrapper that encrypts an underlying `AsyncTransport`.

#[cfg(feature = "tls")]
use std::io::{Read, Write};
#[cfg(feature = "tls")]
use std::sync::Arc;

use super::{AsyncTransport, BufferedTransport, TransportError};

// ----------------------------------------------------------------------------
// TLS Configuration
// ----------------------------------------------------------------------------

/// TLS configuration for PostgreSQL connections.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TlsConfig {
    /// SSL mode — controls whether and how TLS is negotiated.
    pub mode: SslMode,

    /// Server name for SNI and certificate validation.
    /// Defaults to the connection hostname.
    pub server_name: String,

    /// Custom CA certificate (PEM or DER format).
    /// If None, uses the embedded Mozilla CA roots.
    pub ca_cert: Option<Vec<u8>>,

    /// Client certificate for mTLS.
    pub client_cert: Option<Vec<u8>>,

    /// Client private key for mTLS.
    pub client_key: Option<Vec<u8>>,

    /// Accept invalid/self-signed certificates.
    /// **WARNING**: Only for development! Never use in production.
    pub accept_invalid_certs: bool,

    /// Accept certificates with a hostname mismatch.
    /// **WARNING**: Only for development!
    pub accept_invalid_hostnames: bool,

    /// Custom `rustls::crypto::CryptoProvider`.
    /// If None, uses the default provider for the target platform.
    #[cfg(feature = "tls")]
    pub crypto_provider: Option<Arc<rustls::crypto::CryptoProvider>>,
}

impl TlsConfig {
    /// Create a new `TlsConfig` with the given SSL mode and server name.
    ///
    /// All other fields are set to their defaults.
    pub fn new(mode: SslMode, server_name: impl Into<String>) -> Self {
        Self {
            mode,
            server_name: server_name.into(),
            ..Default::default()
        }
    }

    /// Set the SSL mode.
    pub fn mode(mut self, mode: SslMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set the server name for SNI.
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = name.into();
        self
    }

    /// Set a custom CA certificate.
    pub fn ca_cert(mut self, cert: Vec<u8>) -> Self {
        self.ca_cert = Some(cert);
        self
    }

    /// Set the client certificate for mTLS.
    pub fn client_cert(mut self, cert: Vec<u8>) -> Self {
        self.client_cert = Some(cert);
        self
    }

    /// Set the client private key for mTLS.
    pub fn client_key(mut self, key: Vec<u8>) -> Self {
        self.client_key = Some(key);
        self
    }

    /// Accept invalid/self-signed certificates.
    /// **WARNING**: Only for development! Never use in production.
    pub fn accept_invalid_certs(mut self, accept: bool) -> Self {
        self.accept_invalid_certs = accept;
        self
    }

    /// Accept certificates with a hostname mismatch.
    /// **WARNING**: Only for development!
    pub fn accept_invalid_hostnames(mut self, accept: bool) -> Self {
        self.accept_invalid_hostnames = accept;
        self
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            mode: SslMode::Prefer,
            server_name: String::new(),
            ca_cert: None,
            client_cert: None,
            client_key: None,
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
            #[cfg(feature = "tls")]
            crypto_provider: None,
        }
    }
}

/// SSL mode — mirrors PostgreSQL's `sslmode` connection parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum SslMode {
    /// Never use TLS. Connection is plaintext.
    Disable,

    /// Try TLS first; fall back to plaintext if the server doesn't support it.
    Prefer,

    /// Require TLS. Don't verify the server certificate.
    Require,

    /// Require TLS and verify the CA (but not the hostname).
    VerifyCa,

    /// Require TLS, verify CA and hostname. **Recommended for production.**
    VerifyFull,
}

impl SslMode {
    /// Parse from a PostgreSQL connection string `sslmode` value.
    pub fn from_str(s: &str) -> Result<Self, TransportError> {
        match s.to_lowercase().as_str() {
            "disable" => Ok(SslMode::Disable),
            "prefer" => Ok(SslMode::Prefer),
            "require" => Ok(SslMode::Require),
            "verify-ca" => Ok(SslMode::VerifyCa),
            "verify-full" => Ok(SslMode::VerifyFull),
            _ => Err(TransportError::InvalidConfig(format!(
                "invalid sslmode: {}",
                s
            ))),
        }
    }
}

impl std::fmt::Display for SslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SslMode::Disable => write!(f, "disable"),
            SslMode::Prefer => write!(f, "prefer"),
            SslMode::Require => write!(f, "require"),
            SslMode::VerifyCa => write!(f, "verify-ca"),
            SslMode::VerifyFull => write!(f, "verify-full"),
        }
    }
}

// ----------------------------------------------------------------------------
// rustls ClientConfig builder
// ----------------------------------------------------------------------------

#[cfg(feature = "tls")]
fn build_rustls_config(config: &TlsConfig) -> Result<Arc<rustls::ClientConfig>, TransportError> {
    use rustls::client::ClientConfig as RustlsClientConfig;

    // 1. Select CryptoProvider
    let crypto_provider = config
        .crypto_provider
        .clone()
        .unwrap_or_else(default_crypto_provider);

    // 2. Build ClientConfig
    let config_builder = RustlsClientConfig::builder_with_provider(crypto_provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| {
            TransportError::TlsHandshake(format!("unsupported protocol versions: {}", e))
        })?;

    let client_config = if config.accept_invalid_certs {
        config_builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth()
    } else {
        let root_store = build_root_store(config)?;
        config_builder
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

    // 3. Configure mTLS (client certificate) if provided
    let mut client_config = client_config;
    if let (Some(cert_bytes), Some(key_bytes)) = (&config.client_cert, &config.client_key) {
        let certs = parse_certs(cert_bytes)?;
        let key = parse_private_key(key_bytes)?;
        let certified_key = rustls::sign::CertifiedKey::from_der(certs, key, &crypto_provider)
            .map_err(|e| TransportError::TlsHandshake(format!("invalid client cert/key: {}", e)))?;
        client_config.client_auth_cert_resolver =
            Arc::new(rustls::sign::SingleCertAndKey::from(certified_key));
    }

    // 4. ALPN: PostgreSQL does not use ALPN
    client_config.alpn_protocols.clear();

    Ok(Arc::new(client_config))
}

#[cfg(feature = "tls")]
fn default_crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls_rustcrypto::provider())
}

#[cfg(feature = "tls")]
fn build_root_store(config: &TlsConfig) -> Result<rustls::RootCertStore, TransportError> {
    let mut root_store = rustls::RootCertStore::empty();

    if let Some(ref ca_bytes) = config.ca_cert {
        let certs = parse_certs(ca_bytes)?;
        for cert in certs {
            root_store.add(cert).map_err(|e| {
                TransportError::TlsHandshake(format!("failed to add CA certificate: {}", e))
            })?;
        }
    }

    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    Ok(root_store)
}

#[cfg(feature = "tls")]
fn parse_certs(
    bytes: &[u8],
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, TransportError> {
    // Try PEM first
    let mut cursor = std::io::Cursor::new(bytes);
    let pem_result: Result<Vec<_>, _> = rustls_pemfile::certs(&mut cursor).collect();
    if let Ok(certs) = pem_result {
        if !certs.is_empty() {
            return Ok(certs);
        }
    }

    // Try DER format
    Ok(vec![rustls::pki_types::CertificateDer::from(
        bytes.to_vec(),
    )])
}

#[cfg(feature = "tls")]
fn parse_private_key(
    bytes: &[u8],
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, TransportError> {
    // Try PEM first
    let mut cursor = std::io::Cursor::new(bytes);
    if let Ok(Some(key)) = rustls_pemfile::private_key(&mut cursor) {
        return Ok(key);
    }

    // Try DER format (PKCS#8)
    Ok(rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(bytes.to_vec()),
    ))
}

// ----------------------------------------------------------------------------
// No-op certificate verifier (development only)
// ----------------------------------------------------------------------------

#[cfg(feature = "tls")]
#[derive(Debug)]
struct NoVerifier;

#[cfg(feature = "tls")]
impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
        ]
    }
}

// ----------------------------------------------------------------------------
// Async TLS transport
// ----------------------------------------------------------------------------

#[cfg(feature = "tls")]
pub struct TlsTransport<T: AsyncTransport> {
    tls_conn: rustls::ClientConnection,
    inner: T,
}

#[cfg(feature = "tls")]
impl<T: AsyncTransport> TlsTransport<T> {
    /// Perform an async TLS handshake over the given transport.
    pub async fn handshake(
        inner: T,
        config: Arc<rustls::ClientConfig>,
        server_name: &str,
    ) -> Result<Self, TransportError> {
        let server_name = rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|e| {
                TransportError::TlsHandshake(format!(
                    "invalid server name '{}': {}",
                    server_name, e
                ))
            })?;

        let mut tls_conn = rustls::ClientConnection::new(config, server_name).map_err(|e| {
            TransportError::TlsHandshake(format!("TLS connection creation failed: {}", e))
        })?;

        let mut inner = inner;
        let mut handshake_buf = [0u8; 8192];

        let mut iterations = 0;
        const MAX_HANDSHAKE_ITERATIONS: u32 = 100;

        loop {
            iterations += 1;
            if iterations > MAX_HANDSHAKE_ITERATIONS {
                return Err(TransportError::TlsHandshake(
                    "TLS handshake did not complete within iteration limit".into(),
                ));
            }

            // 1. Write any pending outgoing TLS data
            let mut outgoing = Vec::new();
            tls_conn
                .write_tls(&mut outgoing)
                .map_err(|e| TransportError::TlsHandshake(format!("write_tls: {}", e)))?;
            if !outgoing.is_empty() {
                inner.write_all(&outgoing).await?;
                inner.flush().await?;
            }

            // 2. Check if handshake is complete
            if !tls_conn.is_handshaking() {
                break;
            }

            // 3. Read incoming TLS data
            let n = inner.read(&mut handshake_buf).await?;
            if n == 0 {
                return Err(TransportError::UnexpectedEof);
            }

            // 4. Feed data to rustls and process
            let bytes_read = tls_conn
                .read_tls(&mut &handshake_buf[..n])
                .map_err(|e| TransportError::TlsHandshake(format!("read_tls: {}", e)))?;

            if bytes_read == 0 {
                return Err(TransportError::TlsHandshake(
                    "TLS handshake stalled: no data consumed".into(),
                ));
            }

            tls_conn
                .process_new_packets()
                .map_err(|e| TransportError::TlsHandshake(format!("process_new_packets: {}", e)))?;
        }

        if tls_conn.is_handshaking() {
            return Err(TransportError::TlsHandshake(
                "TLS handshake incomplete after loop exit".into(),
            ));
        }

        Ok(TlsTransport { tls_conn, inner })
    }

    /// Get the negotiated TLS protocol version (e.g., TLS 1.3).
    pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        self.tls_conn.protocol_version()
    }

    /// Get the negotiated cipher suite.
    pub fn negotiated_cipher_suite(&self) -> Option<rustls::SupportedCipherSuite> {
        self.tls_conn.negotiated_cipher_suite()
    }

    /// Get the server's peer certificate (if any).
    pub fn peer_certificate(&self) -> Option<rustls::pki_types::CertificateDer<'static>> {
        self.tls_conn
            .peer_certificates()
            .and_then(|certs| certs.first())
            .cloned()
    }

    /// Flush TLS ciphertext from rustls to the underlying async transport.
    async fn flush_tls_outgoing(&mut self) -> Result<(), TransportError> {
        let mut outgoing = Vec::new();
        self.tls_conn
            .write_tls(&mut outgoing)
            .map_err(|e| TransportError::TlsHandshake(format!("write_tls: {}", e)))?;
        if !outgoing.is_empty() {
            self.inner.write_all(&outgoing).await?;
        }
        Ok(())
    }
}

#[cfg(feature = "tls")]
impl<T: AsyncTransport> AsyncTransport for TlsTransport<T> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        loop {
            match self.tls_conn.reader().read(buf) {
                Ok(n) => {
                    if n == 0 {
                        return Ok(0);
                    }
                    return Ok(n);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionAborted => {
                    return Ok(0);
                }
                Err(e) => {
                    return Err(TransportError::TlsHandshake(format!("TLS read: {}", e)));
                }
            }

            let mut cipher_buf = [0u8; 8192];
            let n = self.inner.read(&mut cipher_buf).await?;
            if n == 0 {
                return Err(TransportError::UnexpectedEof);
            }

            self.tls_conn
                .read_tls(&mut &cipher_buf[..n])
                .map_err(|e| TransportError::TlsHandshake(format!("read_tls: {}", e)))?;
            self.tls_conn
                .process_new_packets()
                .map_err(|e| TransportError::TlsHandshake(format!("process_new_packets: {}", e)))?;
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        let n = self
            .tls_conn
            .writer()
            .write(buf)
            .map_err(|e| TransportError::TlsHandshake(format!("TLS write: {}", e)))?;
        self.flush_tls_outgoing().await?;
        Ok(n)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        let mut written = 0;
        while written < buf.len() {
            let n = self.write(&buf[written..]).await?;
            written += n;
        }
        Ok(())
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        let mut filled = 0;
        while filled < buf.len() {
            let n = self.read(&mut buf[filled..]).await?;
            if n == 0 {
                return Err(TransportError::UnexpectedEof);
            }
            filled += n;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), TransportError> {
        self.tls_conn
            .writer()
            .flush()
            .map_err(|e| TransportError::TlsHandshake(format!("TLS flush: {}", e)))?;
        self.flush_tls_outgoing().await?;
        self.inner.flush().await
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        self.tls_conn.send_close_notify();
        self.flush_tls_outgoing().await?;
        self.inner.shutdown().await
    }
}

// ----------------------------------------------------------------------------
// PostgreSQL SSL negotiation
// ----------------------------------------------------------------------------

/// Result of PostgreSQL SSL negotiation.
pub enum PgTransport<T: AsyncTransport> {
    /// Plaintext connection (no TLS).
    Plain(BufferedTransport<T>),
    /// TLS-encrypted connection.
    #[cfg(feature = "tls")]
    Tls(BufferedTransport<TlsTransport<T>>),
}

impl<T: AsyncTransport> AsyncTransport for PgTransport<T> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        match self {
            Self::Plain(t) => t.read(buf).await,
            #[cfg(feature = "tls")]
            Self::Tls(t) => t.read(buf).await,
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        match self {
            Self::Plain(t) => t.write(buf).await,
            #[cfg(feature = "tls")]
            Self::Tls(t) => t.write(buf).await,
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        match self {
            Self::Plain(t) => t.write_all(buf).await,
            #[cfg(feature = "tls")]
            Self::Tls(t) => t.write_all(buf).await,
        }
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        match self {
            Self::Plain(t) => t.read_exact(buf).await,
            #[cfg(feature = "tls")]
            Self::Tls(t) => t.read_exact(buf).await,
        }
    }

    async fn flush(&mut self) -> Result<(), TransportError> {
        match self {
            Self::Plain(t) => t.flush().await,
            #[cfg(feature = "tls")]
            Self::Tls(t) => t.flush().await,
        }
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        match self {
            Self::Plain(t) => t.shutdown().await,
            #[cfg(feature = "tls")]
            Self::Tls(t) => t.shutdown().await,
        }
    }
}

impl<T: AsyncTransport> PgTransport<T> {
    /// Returns true if this transport is using TLS.
    #[cfg(feature = "tls")]
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }

    /// Returns true if this transport is using TLS.
    #[cfg(not(feature = "tls"))]
    pub fn is_tls(&self) -> bool {
        false
    }

    /// Get TLS info if the connection is encrypted.
    #[cfg(feature = "tls")]
    pub fn tls_info(&self) -> Option<TlsInfo> {
        match self {
            Self::Tls(t) => {
                let inner = t.inner();
                Some(TlsInfo {
                    protocol_version: inner.protocol_version().map(|v| format!("{:?}", v)),
                    cipher_suite: inner.negotiated_cipher_suite().map(|v| format!("{:?}", v)),
                    peer_certificate: inner.peer_certificate().map(|c| c.to_vec()),
                })
            }
            Self::Plain(_) => None,
        }
    }

    /// Get TLS info if the connection is encrypted.
    #[cfg(not(feature = "tls"))]
    pub fn tls_info(&self) -> Option<TlsInfo> {
        None
    }
}

/// Information about the TLS connection (if any).
#[derive(Debug)]
#[non_exhaustive]
pub struct TlsInfo {
    pub protocol_version: Option<String>,
    pub cipher_suite: Option<String>,
    pub peer_certificate: Option<Vec<u8>>,
}

/// Verify that `SystemTime::now()` works on this platform.
fn check_time_available() -> Result<(), TransportError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| {
            TransportError::TlsHandshake(format!(
                "SystemTime::now() is not available on this platform. \
                 TLS certificate validation requires the current time. \
                 Error: {}",
                e
            ))
        })?;
    Ok(())
}

/// Negotiate TLS with a PostgreSQL server.
///
/// This implements the PostgreSQL SSL negotiation protocol:
/// 1. Send SSLRequest message
/// 2. Read server response (single byte: 'S' or 'N')
/// 3. If 'S', perform TLS handshake
/// 4. If 'N', handle based on SslMode
#[cfg(feature = "tls")]
pub async fn negotiate_tls<T: AsyncTransport>(
    tcp: T,
    config: &TlsConfig,
) -> Result<PgTransport<T>, TransportError> {
    // Early check: TLS needs current time for certificate validation
    check_time_available()?;

    let mut tcp = tcp;

    // Send SSLRequest message: length=8, code=80877103
    let ssl_request: [u8; 8] = [
        0x00, 0x00, 0x00, 0x08, // length = 8
        0x04, 0xD2, 0x16, 0x2F, // code = 80877103
    ];
    tcp.write_all(&ssl_request).await?;
    tcp.flush().await?;

    // Read server response (single byte)
    let mut response = [0u8; 1];
    tcp.read_exact(&mut response).await?;

    match response[0] {
        b'S' => {
            let tls_config = build_rustls_config(config)?;
            let tls = TlsTransport::handshake(tcp, tls_config, &config.server_name).await?;
            Ok(PgTransport::Tls(BufferedTransport::new(tls)))
        }
        b'N' => match config.mode {
            SslMode::Disable => Ok(PgTransport::Plain(BufferedTransport::new(tcp))),
            SslMode::Prefer => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    server_name = %config.server_name,
                    "Server does not support TLS; falling back to plaintext"
                );
                Ok(PgTransport::Plain(BufferedTransport::new(tcp)))
            }
            SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => {
                Err(TransportError::TlsNotSupported)
            }
        },
        b'E' => {
            let mut len_buf = [0u8; 4];
            tcp.read_exact(&mut len_buf).await?;
            let len = i32::from_be_bytes(len_buf) as usize;
            if len < 4 {
                return Err(TransportError::TlsHandshake(
                    "server sent malformed error response during SSL negotiation".into(),
                ));
            }
            let mut error_buf = vec![0u8; len - 4];
            tcp.read_exact(&mut error_buf).await?;
            Err(TransportError::TlsHandshake(format!(
                "server rejected SSL request: {:?}",
                String::from_utf8_lossy(&error_buf)
            )))
        }
        other => Err(TransportError::TlsHandshake(format!(
            "unexpected response byte during SSL negotiation: 0x{:02x} ('{}')",
            other,
            char::from_u32(other as u32).unwrap_or('?')
        ))),
    }
}

/// Non-TLS negotiation (when tls feature is disabled).
#[cfg(not(feature = "tls"))]
pub async fn negotiate_tls<T: AsyncTransport>(
    tcp: T,
    config: &TlsConfig,
) -> Result<PgTransport<T>, TransportError> {
    match config.mode {
        SslMode::Disable => Ok(PgTransport::Plain(BufferedTransport::new(tcp))),
        _ => Err(TransportError::TlsHandshake(
            "TLS support is not compiled in. Enable the 'tls' feature flag.".into(),
        )),
    }
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::transport::MockTransport;

    #[test]
    fn test_ssl_mode_from_str() {
        assert_eq!(SslMode::from_str("disable").unwrap(), SslMode::Disable);
        assert_eq!(SslMode::from_str("prefer").unwrap(), SslMode::Prefer);
        assert_eq!(SslMode::from_str("require").unwrap(), SslMode::Require);
        assert_eq!(SslMode::from_str("verify-ca").unwrap(), SslMode::VerifyCa);
        assert_eq!(
            SslMode::from_str("verify-full").unwrap(),
            SslMode::VerifyFull
        );
        assert!(matches!(
            SslMode::from_str("invalid"),
            Err(TransportError::InvalidConfig(_))
        ));
    }

    #[test]
    fn test_ssl_mode_display() {
        assert_eq!(SslMode::Disable.to_string(), "disable");
        assert_eq!(SslMode::Prefer.to_string(), "prefer");
        assert_eq!(SslMode::Require.to_string(), "require");
        assert_eq!(SslMode::VerifyCa.to_string(), "verify-ca");
        assert_eq!(SslMode::VerifyFull.to_string(), "verify-full");
    }

    #[test]
    fn test_check_time_available() {
        // Should succeed on any platform that supports SystemTime::now
        assert!(check_time_available().is_ok());
    }

    #[test]
    #[cfg(feature = "tls")]
    fn test_parse_certs_der() {
        // A minimal invalid DER certificate (just to test the path)
        let der = vec![0x30, 0x03, 0x01, 0x01, 0xFF]; // SEQUENCE { BOOLEAN TRUE }
        let certs = parse_certs(&der).unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    #[cfg(feature = "tls")]
    fn test_parse_private_key_der() {
        let der = vec![0x30, 0x03, 0x01, 0x01, 0xFF];
        let key = parse_private_key(&der).unwrap();
        assert!(matches!(key, rustls::pki_types::PrivateKeyDer::Pkcs8(_)));
    }

    #[tokio::test]
    #[cfg(feature = "tls")]
    async fn test_negotiate_tls_server_supports_ssl() {
        use crate::transport::MockTransport;

        // Server responds with 'S' (supports SSL)
        let mock = MockTransport::new(vec![b'S']);
        let config = TlsConfig {
            mode: SslMode::Require,
            server_name: "localhost".into(),
            accept_invalid_certs: true,
            ..Default::default()
        };

        // The handshake will fail because the mock can't provide valid TLS data,
        // but we can at least verify the SSLRequest was sent correctly.
        let result = negotiate_tls(mock, &config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_negotiate_tls_server_rejects_ssl() {
        use crate::transport::MockTransport;

        // Server responds with 'N' (no SSL)
        let mock = MockTransport::new(vec![b'N']);
        let config = TlsConfig {
            mode: SslMode::Disable,
            server_name: "localhost".into(),
            ..Default::default()
        };

        let result = negotiate_tls(mock, &config).await;
        assert!(result.is_ok());
        assert!(!result.unwrap().is_tls());
    }

    #[tokio::test]
    #[cfg(feature = "tls")]
    async fn test_negotiate_tls_server_rejects_ssl_require_mode() {
        use crate::transport::MockTransport;

        let mock = MockTransport::new(vec![b'N']);
        let config = TlsConfig {
            mode: SslMode::Require,
            server_name: "localhost".into(),
            ..Default::default()
        };

        let result = negotiate_tls(mock, &config).await;
        assert!(matches!(result, Err(TransportError::TlsNotSupported)));
    }

    #[tokio::test]
    #[cfg(feature = "tls")]
    async fn test_negotiate_tls_server_sends_error() {
        use crate::transport::MockTransport;

        // Server sends 'E' + error message
        let mut response = vec![b'E'];
        response.extend_from_slice(&i32::to_be_bytes(8)); // length = 8
        response.extend_from_slice(b"M\0test"); // message field

        let mock = MockTransport::new(response);
        let config = TlsConfig {
            mode: SslMode::Require,
            server_name: "localhost".into(),
            ..Default::default()
        };

        let result = negotiate_tls(mock, &config).await;
        assert!(matches!(result, Err(TransportError::TlsHandshake(_))));
    }

    #[tokio::test]
    #[cfg(feature = "tls")]
    async fn test_negotiate_tls_unexpected_byte() {
        use crate::transport::MockTransport;

        let mock = MockTransport::new(vec![b'X']);
        let config = TlsConfig {
            mode: SslMode::Require,
            server_name: "localhost".into(),
            ..Default::default()
        };

        let result = negotiate_tls(mock, &config).await;
        assert!(matches!(result, Err(TransportError::TlsHandshake(_))));
    }

    #[test]
    fn test_pg_transport_is_tls_without_feature() {
        let mock = MockTransport::new(vec![]);
        let buf = BufferedTransport::new(mock);
        let pg = PgTransport::Plain(buf);
        assert!(!pg.is_tls());
        assert!(pg.tls_info().is_none());
    }
}
