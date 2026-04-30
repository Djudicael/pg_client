# Step 03 - TLS Support (Async)

## Goal
Implement async TLS encryption over the TCP transport using `rustls` (pure-Rust TLS), with PostgreSQL's specific TLS negotiation flow, robust error handling, and WASI P2 compatibility.

## Context
PostgreSQL has a unique TLS handshake:
1. Client sends an `SSLRequest` message (8 bytes: length=8, code=80877103)
2. Server responds with a **single byte**: `S` (supports SSL) or `N` (no SSL)
3. If `S`, client initiates standard TLS handshake over the same TCP connection
4. After TLS handshake, normal PostgreSQL startup continues encrypted

This is NOT standard STARTTLS — the SSLRequest is PostgreSQL-specific.

### WASI P2 TLS Challenges

TLS on WASI P2 has three critical dependencies that must all work:

1. **`rustls` CryptoProvider**: `ring` (the default) uses platform-specific assembly and may not compile for `wasm32-wasip2`. We use `rustls-rustcrypto` (pure Rust) as the default WASI provider, with `ring` as an optional alternative for native builds.

2. **`getrandom`**: TLS needs a CSPRNG for key generation, nonce creation, etc. The `getrandom` crate must be configured with the `wasi` feature to use `wasi:random/random`. If misconfigured, TLS will panic at runtime with an opaque error. See Step 01 for the configuration strategy.

3. **Time**: TLS certificate validation requires the current time. On WASI P2, `std::time::SystemTime::now()` works via `wasi:clocks/wall-clock`. If it doesn't, certificate validation will fail.

## Tasks

### 3.1 - Integrate `rustls`

```toml
[dependencies]
# TLS support (behind "tls" feature flag)
rustls = { version = "0.23", default-features = false, features = ["std", "tls12"], optional = true }
rustls-rustcrypto = { version = "0.0.2", optional = true }  # Pure-Rust CryptoProvider for WASI
webpki-roots = { version = "0.26", optional = true }        # Mozilla CA roots (embedded)

# Required for TLS randomness
getrandom = { version = "0.4" }  # v0.4+ auto-detects WASI P2
```

**Crypto provider strategy**:

```
┌──────────────────────────────────────────────────────────────┐
│ Build time (feature flag)                                     │
│   tls = ["dep:rustls", "dep:rustls-rustcrypto", ...]        │
├──────────────────────────────────────────────────────────────┤
│ Runtime: select CryptoProvider                                │
│   1. User-provided (via TlsConfig::crypto_provider)          │
│   2. rustls-rustcrypto (default for WASI, pure Rust)         │
│   3. ring (if user enables it manually for native builds)    │
├──────────────────────────────────────────────────────────────┤
│ Fallback: compile without "tls" feature                       │
│   Only plaintext connections (SslMode::Disable only)          │
│   Clear compile-time error if user tries SslMode::Require    │
└──────────────────────────────────────────────────────────────┘
```

**`rustls-rustcrypto` risk assessment**:

| Concern | Severity | Mitigation |
|---------|----------|------------|
| Immature (v0.0.x) — may have bugs | High | Fallback: plaintext-only mode. Allow user-provided `CryptoProvider`. |
| Missing cipher suites | Medium | Test against PostgreSQL 12–16. Document which suites are supported. |
| Performance (pure Rust vs assembly) | Low | TLS handshake is one-time per connection; acceptable overhead for a DB client. |
| May not compile on future rustls versions | Medium | Pin `rustls = "0.23"` and `rustls-rustcrypto = "0.0.2"`. Test upgrades manually. |

### 3.2 - TLS configuration

```rust
/// TLS configuration for PostgreSQL connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// SSL mode — controls whether and how TLS is negotiated.
    pub mode: SslMode,

    /// Server name for SNI (Server Name Indication) and certificate validation.
    /// Defaults to the connection hostname.
    pub server_name: String,

    /// Custom CA certificate (PEM or DER format).
    /// If None, uses the embedded Mozilla CA roots.
    pub ca_cert: Option<Vec<u8>>,

    /// Client certificate for mTLS (mutual TLS authentication).
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
    /// If None, uses the default provider for the target platform:
    ///   - wasm32-wasip2: rustls-rustcrypto
    ///   - native: ring (if available) or rustls-rustcrypto
    pub crypto_provider: Option<Arc<rustls::crypto::CryptoProvider>>,
}

/// SSL mode — mirrors PostgreSQL's `sslmode` connection parameter.
///
/// The modes are ordered by security level (least to most secure).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum SslMode {
    /// Never use TLS. Connection is plaintext.
    Disable,

    /// Try TLS first; fall back to plaintext if the server doesn't support it.
    /// **Not recommended** — the fallback to plaintext is silent and exposes credentials.
    Prefer,

    /// Require TLS. Don't verify the server certificate.
    /// Protects against passive eavesdropping but NOT man-in-the-middle attacks.
    Require,

    /// Require TLS and verify the CA (but not the hostname).
    VerifyCa,

    /// Require TLS, verify CA and hostname. **Recommended for production.**
    VerifyFull,
}

impl SslMode {
    /// Parse from a PostgreSQL connection string `sslmode` value.
    pub fn from_str(s: &str) -> Result<Self, ConfigError> {
        match s.to_lowercase().as_str() {
            "disable" => Ok(SslMode::Disable),
            "prefer" => Ok(SslMode::Prefer),
            "require" => Ok(SslMode::Require),
            "verify-ca" => Ok(SslMode::VerifyCa),
            "verify-full" => Ok(SslMode::VerifyFull),
            _ => Err(ConfigError::InvalidSslMode(s.to_string())),
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
```

### 3.3 - Build `rustls::ClientConfig`

```rust
fn build_rustls_config(config: &TlsConfig) -> Result<Arc<rustls::ClientConfig>, TransportError> {
    // 1. Select CryptoProvider
    let crypto_provider = config.crypto_provider.clone()
        .unwrap_or_else(default_crypto_provider);

    // 2. Build ClientConfig based on SslMode
    let config_builder = rustls::ClientConfig::builder_with_provider(crypto_provider)
        .with_protocol_versions(&[
            &rustls::version::TLS13,  // Preferred
            &rustls::version::TLS12,  // Fallback
        ])
        .map_err(|e| TransportError::TlsHandshake(format!("unsupported protocol versions: {}", e)))?;

    let client_config = if config.accept_invalid_certs {
        // DANGER: No certificate verification. Development only.
        config_builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth()
    } else {
        // Build root certificate store
        let root_store = build_root_store(config)?;

        let builder = config_builder
            .with_root_certificates(root_store)
            .with_no_client_auth();

        builder
    };

    // 3. Configure mTLS (client certificate) if provided
    let mut client_config = client_config;
    if let (Some(cert_bytes), Some(key_bytes)) = (&config.client_cert, &config.client_key) {
        let certs = parse_certs(cert_bytes)?;
        let key = parse_private_key(key_bytes)?;
        client_config.client_auth_cert_resolver = Arc::new(
            rustls::client::ResolvesClientCert::always(certs, key)
        );
    }

    // 4. ALPN: PostgreSQL does not use ALPN, but we set it to empty
    //    to avoid sending any ALPN extension (some servers reject unknown ALPN).
    client_config.alpn_protocols.clear();

    // 5. Configure SNI
    //    SNI is sent automatically by rustls based on the ServerName.
    //    We control this in the handshake function.

    Ok(Arc::new(client_config))
}

/// Default CryptoProvider for the current target.
fn default_crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    // Use rustls-rustcrypto (pure Rust, guaranteed to compile on WASI).
    // Users can override this via TlsConfig::crypto_provider.
    Arc::new(rustls_rustcrypto::provider())
}

/// Build the root certificate store.
fn build_root_store(config: &TlsConfig) -> Result<rustls::RootCertStore, TransportError> {
    let mut root_store = rustls::RootCertStore::empty();

    // Add custom CA certificate if provided
    if let Some(ref ca_bytes) = config.ca_cert {
        let certs = parse_certs(ca_bytes)?;
        for cert in certs {
            root_store.add(cert)
                .map_err(|e| TransportError::TlsHandshake(
                    format!("failed to add CA certificate: {}", e)
                ))?;
        }
    }

    // Add embedded Mozilla CA roots
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    Ok(root_store)
}

/// Parse PEM or DER encoded certificates.
fn parse_certs(bytes: &[u8]) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, TransportError> {
    // Try PEM first (most common format)
    let pem_result: Result<Vec<_>, _> = rustls_pemfile::certs(&mut &bytes[..])
        .collect();
    if let Ok(certs) = pem_result {
        if !certs.is_empty() {
            return Ok(certs);
        }
    }

    // Try DER format
    Ok(vec![rustls::pki_types::CertificateDer::from(bytes.to_vec())])
}

/// Parse PEM or DER encoded private key.
fn parse_private_key(bytes: &[u8]) -> Result<rustls::pki_types::PrivateKeyDer<'static>, TransportError> {
    // Try PEM first
    let pem_result = rustls_pemfile::private_key(&mut &bytes[..]);
    if let Ok(Some(key)) = pem_result {
        return Ok(key);
    }

    // Try DER format (PKCS#8)
    Ok(rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(bytes.to_vec())
    ))
}

/// No-op certificate verifier for development/testing.
/// **WARNING**: This disables ALL certificate validation.
struct NoVerifier;

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
```

### 3.4 - Async TLS transport wrapper

```rust
/// Async TLS transport that wraps an inner async transport with rustls.
///
/// The TLS state machine is driven by reading/writing ciphertext from/to
/// the inner transport, and presenting plaintext read/write to the caller.
pub struct TlsTransport<T: AsyncTransport> {
    tls_conn: rustls::ClientConnection,
    inner: T,
}

impl<T: AsyncTransport> TlsTransport<T> {
    /// Perform an async TLS handshake over the given transport.
    ///
    /// This drives the TLS state machine by:
    /// 1. Writing pending TLS handshake data to the inner transport
    /// 2. Reading TLS handshake data from the inner transport
    /// 3. Processing the data through rustls
    /// 4. Repeating until the handshake is complete
    pub async fn handshake(
        inner: T,
        config: Arc<rustls::ClientConfig>,
        server_name: &str,
    ) -> Result<Self, TransportError> {
        let server_name = rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|e| TransportError::TlsHandshake(
                format!("invalid server name '{}': {}", server_name, e)
            ))?;

        let mut tls_conn = rustls::ClientConnection::new(config, server_name)
            .map_err(|e| TransportError::TlsHandshake(
                format!("TLS connection creation failed: {}", e)
            ))?;

        let mut inner = inner;
        let mut handshake_buf = [0u8; 8192];

        // Drive the TLS handshake to completion.
        // This loop handles the multi-step TLS handshake:
        //   ClientHello → ServerHello → Certificate → ... → Finished
        //
        // Each iteration:
        //   1. Write any pending outgoing TLS data
        //   2. If handshake is complete, break
        //   3. Read incoming TLS data
        //   4. Process it through rustls
        let mut iterations = 0;
        const MAX_HANDSHAKE_ITERATIONS: u32 = 100; // Safety limit

        loop {
            iterations += 1;
            if iterations > MAX_HANDSHAKE_ITERATIONS {
                return Err(TransportError::TlsHandshake(
                    "TLS handshake did not complete within iteration limit".into()
                ));
            }

            // 1. Write any pending outgoing TLS data
            let mut outgoing = Vec::new();
            tls_conn.write_tls(&mut outgoing)
                .map_err(|e| TransportError::TlsHandshake(
                    format!("failed to serialize TLS data: {}", e)
                ))?;
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
            let mut cursor = std::io::Cursor::new(&handshake_buf[..n]);
            let bytes_read = tls_conn.read_tls(&mut cursor)
                .map_err(|e| TransportError::TlsHandshake(
                    format!("failed to read TLS data: {}", e)
                ))?;

            if bytes_read == 0 && cursor.position() == 0 {
                // No data was consumed — this shouldn't happen but guard against it
                return Err(TransportError::TlsHandshake(
                    "TLS handshake stalled: no data consumed".into()
                ));
            }

            tls_conn.process_new_packets()
                .map_err(|e| TransportError::TlsHandshake(
                    format!("TLS packet processing failed: {}", e)
                ))?;
        }

        // Verify the handshake completed successfully
        if tls_conn.is_handshaking() {
            return Err(TransportError::TlsHandshake(
                "TLS handshake incomplete after loop exit".into()
            ));
        }

        Ok(TlsTransport { tls_conn, inner })
    }
}

impl<T: AsyncTransport> AsyncTransport for TlsTransport<T> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        // Loop to drive the TLS state machine:
        // 1. Try to read decrypted plaintext from rustls
        // 2. If WouldBlock, read more ciphertext from the inner transport
        // 3. Process the ciphertext through rustls
        // 4. Repeat
        loop {
            // 1. Try to read decrypted plaintext
            match self.tls_conn.reader().read(buf) {
                Ok(n) => {
                    if n == 0 {
                        // TLS connection closed cleanly
                        return Ok(0);
                    }
                    return Ok(n);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Need more ciphertext from the network — fall through
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionAborted => {
                    // TLS connection was closed by the peer
                    return Ok(0);
                }
                Err(e) => {
                    return Err(TransportError::TlsHandshake(
                        format!("TLS read error: {}", e)
                    ));
                }
            }

            // 2. Read ciphertext from the inner transport
            let mut cipher_buf = [0u8; 8192];
            let n = self.inner.read(&mut cipher_buf).await?;
            if n == 0 {
                return Err(TransportError::UnexpectedEof);
            }

            // 3. Feed ciphertext to rustls
            self.tls_conn.read_tls(&mut &cipher_buf[..n])
                .map_err(|e| TransportError::TlsHandshake(
                    format!("failed to read TLS record: {}", e)
                ))?;

            // 4. Process new packets (may produce plaintext or require more data)
            self.tls_conn.process_new_packets()
                .map_err(|e| TransportError::TlsHandshake(
                    format!("TLS packet processing failed: {}", e)
                ))?;
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        // 1. Write plaintext into the TLS connection
        let n = self.tls_conn.writer().write(buf)
            .map_err(|e| TransportError::TlsHandshake(
                format!("TLS write error: {}", e)
            ))?;

        // 2. Flush ciphertext to the underlying transport
        self.flush_tls_outgoing().await?;

        Ok(n)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        // Write all plaintext, flushing in chunks to avoid large buffers
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
        // Flush the TLS writer (may produce more ciphertext)
        self.tls_conn.writer().flush()
            .map_err(|e| TransportError::TlsHandshake(
                format!("TLS flush error: {}", e)
            ))?;

        // Flush any pending ciphertext to the inner transport
        self.flush_tls_outgoing().await?;

        // Flush the inner transport
        self.inner.flush().await
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        // Send TLS close_notify
        self.tls_conn.send_close_notify();

        // Flush any remaining ciphertext (including close_notify)
        self.flush_tls_outgoing().await?;

        // Shut down the inner transport
        self.inner.shutdown().await
    }
}

impl<T: AsyncTransport> TlsTransport<T> {
    /// Flush TLS ciphertext from rustls to the underlying async transport.
    async fn flush_tls_outgoing(&mut self) -> Result<(), TransportError> {
        let mut outgoing = Vec::new();
        self.tls_conn.write_tls(&mut outgoing)
            .map_err(|e| TransportError::TlsHandshake(
                format!("failed to serialize TLS data: {}", e)
            ))?;
        if !outgoing.is_empty() {
            self.inner.write_all(&outgoing).await?;
        }
        Ok(())
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
    pub fn peer_certificate(&self) -> Option<rustls::pki_types::CertificateDer<'_>> {
        self.tls_conn.peer_certificates()
            .and_then(|certs| certs.first())
            .cloned()
    }
}
```

### 3.5 - PostgreSQL SSL negotiation (async)

```rust
/// Result of PostgreSQL SSL negotiation.
pub enum PgTransport<T: AsyncTransport = WasiTcpTransport> {
    /// Plaintext connection (no TLS).
    Plain(BufferedTransport<T>),
    /// TLS-encrypted connection.
    Tls(BufferedTransport<TlsTransport<T>>),
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
    let mut tcp = tcp;

    // 1. Send SSLRequest message
    //    Format: length(i32=8) + ssl_request_code(i32=80877103)
    let ssl_request: [u8; 8] = [
        0x00, 0x00, 0x00, 0x08,  // length = 8
        0x04, 0xD2, 0x16, 0x2F,  // code = 80877103
    ];
    tcp.write_all(&ssl_request).await?;
    tcp.flush().await?;

    // 2. Read server response (single byte)
    let mut response = [0u8; 1];
    tcp.read_exact(&mut response).await?;

    match response[0] {
        b'S' => {
            // Server supports SSL — initiate TLS handshake
            let tls_config = build_rustls_config(config)?;
            let tls = TlsTransport::handshake(tcp, tls_config, &config.server_name).await?;
            Ok(PgTransport::Tls(BufferedTransport::new(tls)))
        }
        b'N' => {
            // Server does not support SSL
            match config.mode {
                SslMode::Disable => {
                    // Expected — no TLS wanted
                    Ok(PgTransport::Plain(BufferedTransport::new(tcp)))
                }
                SslMode::Prefer => {
                    // Acceptable — fall back to plaintext
                    // Log a warning if tracing is enabled
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        server_name = %config.server_name,
                        "Server does not support TLS; falling back to plaintext connection"
                    );
                    Ok(PgTransport::Plain(BufferedTransport::new(tcp)))
                }
                SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => {
                    // Not acceptable — TLS was required
                    Err(TransportError::TlsNotSupported)
                }
            }
        }
        b'E' => {
            // Server sent an error response instead of S/N.
            // This can happen if the server doesn't recognize the SSLRequest.
            // Read the full ErrorResponse message and return it.
            // The error message follows the standard PostgreSQL error format:
            // type('E') + length(i32) + fields
            // We've already read the 'E' byte, so read the length + fields.
            let mut len_buf = [0u8; 4];
            tcp.read_exact(&mut len_buf).await?;
            let len = i32::from_be_bytes(len_buf) as usize;
            if len < 4 {
                return Err(TransportError::TlsHandshake(
                    "server sent malformed error response during SSL negotiation".into()
                ));
            }
            let mut error_buf = vec![0u8; len - 4];
            tcp.read_exact(&mut error_buf).await?;

            Err(TransportError::TlsHandshake(
                format!("server rejected SSL request: {:?}", String::from_utf8_lossy(&error_buf))
            ))
        }
        other => {
            // Unexpected response byte
            Err(TransportError::TlsHandshake(
                format!(
                    "unexpected response byte during SSL negotiation: 0x{:02x} ('{}')",
                    other,
                    char::from_u32(other as u32).unwrap_or('?')
                )
            ))
        }
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
            "TLS support is not compiled in. Enable the 'tls' feature flag.".into()
        )),
    }
}
```

### 3.6 - `PgTransport` async delegation

```rust
impl<T: AsyncTransport> AsyncTransport for PgTransport<T> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        match self {
            Self::Plain(t) => t.read(buf).await,
            Self::Tls(t) => t.read(buf).await,
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        match self {
            Self::Plain(t) => t.write(buf).await,
            Self::Tls(t) => t.write(buf).await,
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        match self {
            Self::Plain(t) => t.write_all(buf).await,
            Self::Tls(t) => t.write_all(buf).await,
        }
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        match self {
            Self::Plain(t) => t.read_exact(buf).await,
            Self::Tls(t) => t.read_exact(buf).await,
        }
    }

    async fn flush(&mut self) -> Result<(), TransportError> {
        match self {
            Self::Plain(t) => t.flush().await,
            Self::Tls(t) => t.flush().await,
        }
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        match self {
            Self::Plain(t) => t.shutdown().await,
            Self::Tls(t) => t.shutdown().await,
        }
    }
}

impl<T: AsyncTransport> PgTransport<T> {
    /// Returns true if this transport is using TLS.
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }

    /// Get TLS info if the connection is encrypted.
    #[cfg(feature = "tls")]
    pub fn tls_info(&self) -> Option<TlsInfo> {
        match self {
            Self::Tls(t) => {
                let tls_conn = &t.get_ref().tls_conn;
                Some(TlsInfo {
                    protocol_version: tls_conn.protocol_version(),
                    cipher_suite: tls_conn.negotiated_cipher_suite(),
                    peer_certificate: tls_conn.peer_certificates()
                        .and_then(|certs| certs.first())
                        .cloned(),
                    server_name: tls_conn.server_name().map(|s| s.to_string()),
                })
            }
            Self::Plain(_) => None,
        }
    }
}

/// Information about the TLS connection (if any).
#[cfg(feature = "tls")]
#[derive(Debug)]
pub struct TlsInfo {
    pub protocol_version: Option<rustls::ProtocolVersion>,
    pub cipher_suite: Option<rustls::SupportedCipherSuite>,
    pub peer_certificate: Option<rustls::pki_types::CertificateDer<'static>>,
    pub server_name: Option<String>,
}
```

### 3.7 - Certificate revocation (future enhancement)

Certificate revocation checking (CRL/OCSP) is not implemented in v0.1. `rustls` supports CRL checking via `rustls::client::danger::ServerCertVerifier` but it requires fetching CRLs, which adds complexity (HTTP client, caching, freshness).

For v0.1, we rely on:
- Certificate expiration checking (built into rustls)
- Hostname verification (built into rustls)
- CA chain verification (built into rustls)

For v0.2, we can add:
- CRL distribution point fetching
- OCSP stapling support
- Custom certificate verifier hooks

### 3.8 - ALPN consideration

PostgreSQL does not use ALPN (Application-Layer Protocol Negotiation). We explicitly clear the ALPN list in the `ClientConfig` to avoid sending any ALPN extension. Some PostgreSQL servers or proxies (like PgBouncer) may reject connections with unexpected ALPN values.

```rust
// In build_rustls_config():
client_config.alpn_protocols.clear();
```

### 3.9 - Time availability on WASI P2

TLS certificate validation requires the current time to check certificate validity periods. On `wasm32-wasip2`, `std::time::SystemTime::now()` works via `wasi:clocks/wall-clock`.

If `SystemTime::now()` is not available or returns an error, `rustls` will fail certificate validation. We add a runtime check during TLS setup:

```rust
/// Verify that SystemTime::now() works on this platform.
/// TLS certificate validation requires the current time.
fn check_time_available() -> Result<(), TransportError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| TransportError::TlsHandshake(
            format!(
                "SystemTime::now() is not available on this platform. \
                TLS certificate validation requires the current time. \
                Error: {}", e
            )
        ))?;
    Ok(())
}
```

This check is called at the beginning of `negotiate_tls()` to provide an early, actionable error message instead of a confusing "invalid certificate" error later.

## File Layout

```
crates/pg-client/src/
├── transport/
│   ├── mod.rs          (AsyncTransport trait + PgTransport enum + re-exports)
│   ├── tcp.rs          (WasiTcpTransport using wstd::net::TcpStream)
│   ├── tls.rs          (TlsTransport, async handshake, negotiate_tls, TlsInfo)
│   ├── buffered.rs     (BufferedTransport)
│   ├── native.rs       (NativeTcpTransport — test-native feature only)
│   ├── raw_wasi.rs     (RawWasiTransport — fallback, not compiled by default)
│   ├── config.rs       (TlsConfig, SslMode)
│   ├── error.rs        (TransportError)
│   └── params.rs       (ConnectionParams)
```

## Acceptance Criteria

- [ ] Async TLS handshake succeeds with a PostgreSQL server configured for SSL
- [ ] All SslMode variants work correctly (Disable, Prefer, Require, VerifyCa, VerifyFull)
- [ ] Certificate verification works (CA chain, hostname, expiration)
- [ ] Graceful fallback in `Prefer` mode (with tracing warning)
- [ ] Custom CA certs supported (PEM and DER format)
- [ ] mTLS (client certificates) supported
- [ ] `accept_invalid_certs` works for development (with clear documentation warning)
- [ ] Pure-Rust crypto via `rustls-rustcrypto` (no `ring` dependency by default)
- [ ] User can provide custom `CryptoProvider` for alternative backends
- [ ] `getrandom` properly configured for WASI P2 (v0.3+ with `wasi` feature)
- [ ] `SystemTime::now()` availability checked before TLS negotiation
- [ ] ALPN list cleared (PostgreSQL doesn't use ALPN)
- [ ] TLS close_notify sent on shutdown
- [ ] TLS info (protocol version, cipher suite, peer cert) accessible after connection
- [ ] Error response during SSL negotiation handled (server sends 'E' byte)
- [ ] Handshake iteration limit prevents infinite loops
- [ ] Compiles to `wasm32-wasip2`
- [ ] Compiles without `tls` feature (plaintext-only mode with clear error)

## Key Risks and Mitigations

| Risk | Severity | Mitigation |
|------|----------|------------|
| `rustls-rustcrypto` immaturity (v0.0.x) | High | Allow user-provided `CryptoProvider`. Support plaintext-only mode. Test against PG 12–16. |
| `rustls-rustcrypto` missing cipher suites | Medium | Test which suites are available. Document limitations. Fall back to TLS 1.2 if 1.3 suites are missing. |
| `getrandom` misconfiguration | High | Pin v0.3+ with `wasi` feature. Add runtime sanity check (Step 01). Document in README. |
| `SystemTime::now()` unavailable on WASI | Medium | Add early runtime check with actionable error message. Allow `accept_invalid_certs` as workaround. |
| TLS handshake stalls (network issues) | Low | Iteration limit (100) prevents infinite loops. Timeout via `connect_with_timeout` wraps the whole process. |
| `rustls` API breaking changes | Medium | Pin `rustls = "0.23"`. Test upgrades manually. |
| `rustls-pemfile` not available on WASI | Low | Include it as a dependency. It's pure Rust with no platform-specific code. |

## Testing

- **Unit test**: SSL negotiation with mock transport responding `S` / `N` / `E`
- **Unit test**: `SslMode::from_str()` for all valid and invalid values
- **Unit test**: `build_rustls_config()` with various TlsConfig combinations
- **Unit test**: `NoVerifier` compiles and passes verification
- **Unit test**: Certificate parsing (PEM and DER)
- **Integration test**: Connect to PostgreSQL with `sslmode=require`
- **Integration test**: Connect to PostgreSQL with `sslmode=verify-full` (with valid CA)
- **Integration test**: Certificate verification failure (self-signed cert, expired cert)
- **Integration test**: Hostname mismatch detection
- **Integration test**: mTLS with client certificate
- **Integration test**: TLS 1.2 and TLS 1.3 negotiation
- **Integration test**: Graceful fallback in `Prefer` mode
- **Integration test**: Error when server doesn't support SSL and mode is `Require`
- **Integration test**: TLS info accessible after connection
- **Integration test**: TLS close_notify sent on clean shutdown
- **WASI E2E test**: Full TLS connection from WASI component to PostgreSQL
