# Step 05 - Authentication (Async)

## Goal
Implement all major PostgreSQL authentication mechanisms: Trust, Cleartext Password, MD5, and SCRAM-SHA-256 (the modern default). All network I/O is async.

## Context
After the StartupMessage, the server responds with an Authentication message. The client must handle the specific auth method the server requires. Modern PostgreSQL defaults to SCRAM-SHA-256 (since PG 10+).

All crypto must be **pure Rust** for WASI P2 compatibility. The auth flow itself is async because it reads/writes from the transport.

## Tasks

### 5.1 - Async authentication dispatcher
```rust
pub async fn authenticate(
    transport: &mut impl AsyncTransport,
    codec: &mut Codec,
    params: &ConnectionParams,
) -> Result<ServerParams, AuthError> {
    loop {
        let msg = codec.read_message(transport).await?;
        match msg {
            BackendMessage::AuthenticationOk => break,
            BackendMessage::AuthenticationCleartextPassword => {
                auth_cleartext(transport, codec, params).await?;
            }
            BackendMessage::AuthenticationMD5Password { salt } => {
                auth_md5(transport, codec, params, &salt).await?;
            }
            BackendMessage::AuthenticationSASL { mechanisms } => {
                auth_sasl(transport, codec, params, &mechanisms).await?;
            }
            BackendMessage::ErrorResponse { fields } => {
                return Err(AuthError::ServerError(PgError::from_fields(fields)));
            }
            other => return Err(AuthError::UnexpectedMessage(other)),
        }
    }
    // After AuthenticationOk, read ParameterStatus + BackendKeyData until ReadyForQuery
    read_startup_params(transport, codec).await
}
```

### 5.2 - Cleartext password (async)
```rust
async fn auth_cleartext(
    transport: &mut impl AsyncTransport,
    codec: &mut Codec,
    params: &ConnectionParams,
) -> Result<(), AuthError> {
    let password = params.password.as_ref().ok_or(AuthError::PasswordRequired)?;
    codec.send(transport, &FrontendMessage::PasswordMessage {
        password: password.clone(),
    }).await
}
```

### 5.3 - MD5 password (async)
```rust
// PostgreSQL MD5 auth: md5(md5(password + user) + salt)
async fn auth_md5(
    transport: &mut impl AsyncTransport,
    codec: &mut Codec,
    params: &ConnectionParams,
    salt: &[u8; 4],
) -> Result<(), AuthError> {
    let password = params.password.as_ref().ok_or(AuthError::PasswordRequired)?;

    // Step 1: md5(password + username)
    let mut hasher = Md5::new();
    hasher.update(password.as_bytes());
    hasher.update(params.user.as_bytes());
    let inner = format!("{:x}", hasher.finalize());

    // Step 2: md5(inner_hex + salt)
    let mut hasher = Md5::new();
    hasher.update(inner.as_bytes());
    hasher.update(salt);
    let outer = format!("md5{:x}", hasher.finalize());

    codec.send(transport, &FrontendMessage::PasswordMessage { password: outer }).await
}
```

### 5.4 - SCRAM-SHA-256 (RFC 5802 / 7677) - async
This is the most complex auth method. It's a 3-step challenge-response.
The crypto is sync (pure computation), but the message send/receive is async.

**Step 1: Client sends SASLInitialResponse**
```rust
async fn auth_sasl(
    transport: &mut impl AsyncTransport,
    codec: &mut Codec,
    params: &ConnectionParams,
    mechanisms: &[String],
) -> Result<(), AuthError> {
    // Verify SCRAM-SHA-256 is supported
    if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
        return Err(AuthError::UnsupportedMechanism(mechanisms.clone()));
    }

    let password = params.password.as_ref().ok_or(AuthError::PasswordRequired)?;

    // Step 1: Generate client-first message
    let nonce = generate_nonce();  // sync, uses wstd::rand
    let client_first_bare = format!("n={},r={}", sasl_prep(&params.user), nonce);
    let client_first = format!("n,,{}", client_first_bare);

    codec.send(transport, &FrontendMessage::SASLInitialResponse {
        mechanism: "SCRAM-SHA-256".to_string(),
        data: client_first.into_bytes(),
    }).await?;

    // Step 2: Read server-first, compute proof, send client-final
    let msg = codec.read_message(transport).await?;
    let server_first = match msg {
        BackendMessage::AuthenticationSASLContinue { data } => data,
        other => return Err(AuthError::UnexpectedMessage(other)),
    };

    let (client_final, server_signature) = scram_compute_client_final(
        password,
        &client_first_bare,
        &server_first,
        &nonce,
    )?;

    codec.send(transport, &FrontendMessage::SASLResponse {
        data: client_final.into_bytes(),
    }).await?;

    // Step 3: Verify server-final
    let msg = codec.read_message(transport).await?;
    let server_final = match msg {
        BackendMessage::AuthenticationSASLFinal { data } => data,
        other => return Err(AuthError::UnexpectedMessage(other)),
    };

    scram_verify_server_final(&server_final, &server_signature)?;

    Ok(())
}
```

**SCRAM computation (sync - pure crypto, no I/O)**
```rust
fn scram_compute_client_final(
    password: &str,
    client_first_bare: &str,
    server_first: &[u8],
    client_nonce: &str,
) -> Result<(String, Vec<u8>), AuthError> {
    let server_first_str = std::str::from_utf8(server_first)?;

    // Parse server-first-message: r=<nonce>, s=<salt>, i=<iterations>
    let server_nonce = parse_scram_field(server_first_str, "r=")?;
    let salt = base64::decode(parse_scram_field(server_first_str, "s=")?)?;
    let iterations: u32 = parse_scram_field(server_first_str, "i=")?.parse()?;

    // Verify server nonce starts with our nonce
    if !server_nonce.starts_with(client_nonce) {
        return Err(AuthError::ScramNonceMismatch);
    }

    // Derive keys (sync computation)
    let mut salted_password = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<sha2::Sha256>(
        password.as_bytes(), &salt, iterations, &mut salted_password,
    );

    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let stored_key = sha256(&client_key);
    let server_key = hmac_sha256(&salted_password, b"Server Key");

    let client_final_without_proof = format!("c=biws,r={}", server_nonce);
    let auth_message = format!(
        "{},{},{}", client_first_bare, server_first_str, client_final_without_proof
    );

    let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
    let proof = xor_bytes(&client_key, &client_signature);
    let client_final = format!("{},p={}", client_final_without_proof, base64::encode(&proof));

    let server_signature = hmac_sha256(&server_key, auth_message.as_bytes());

    Ok((client_final, server_signature.to_vec()))
}

fn scram_verify_server_final(server_final: &[u8], expected_sig: &[u8]) -> Result<(), AuthError> {
    let server_final_str = std::str::from_utf8(server_final)?;
    let sig = base64::decode(parse_scram_field(server_final_str, "v=")?)?;
    if sig != expected_sig {
        return Err(AuthError::ServerSignatureMismatch);
    }
    Ok(())
}
```

### 5.5 - Random nonce generation
```rust
fn generate_nonce() -> String {
    // Use wstd::rand or wasi:random/random for 24 cryptographic random bytes
    let random_bytes: Vec<u8> = (0..24).map(|_| wstd::rand::random::<u8>()).collect();
    base64::encode(&random_bytes)
}
```

### 5.6 - Async startup parameter collection
```rust
pub struct ServerParams {
    pub process_id: i32,
    pub secret_key: i32,
    pub server_version: String,
    pub server_encoding: String,
    pub client_encoding: String,
    pub params: HashMap<String, String>,
}

async fn read_startup_params(
    transport: &mut impl AsyncTransport,
    codec: &mut Codec,
) -> Result<ServerParams, AuthError> {
    let mut params = ServerParams::default();

    loop {
        let msg = codec.read_message(transport).await?;
        match msg {
            BackendMessage::BackendKeyData { process_id, secret_key } => {
                params.process_id = process_id;
                params.secret_key = secret_key;
            }
            BackendMessage::ParameterStatus { name, value } => {
                match name.as_str() {
                    "server_version" => params.server_version = value.clone(),
                    "server_encoding" => params.server_encoding = value.clone(),
                    "client_encoding" => params.client_encoding = value.clone(),
                    _ => {}
                }
                params.params.insert(name, value);
            }
            BackendMessage::ReadyForQuery { .. } => break,
            BackendMessage::ErrorResponse { fields } => {
                return Err(AuthError::ServerError(PgError::from_fields(fields)));
            }
            _ => {}
        }
    }

    Ok(params)
}
```

## Dependencies
```toml
md-5 = "0.10"       # MD5 hashing (optional, behind feature flag)
sha2 = "0.10"       # SHA-256
hmac = "0.12"       # HMAC
pbkdf2 = "0.12"     # PBKDF2 key derivation
base64 = "0.22"     # Base64 encoding/decoding
```
All are pure Rust, WASI-compatible, and sync (no I/O).

## File Layout
```
crates/pg-client/src/
├── auth/
│   ├── mod.rs           (async authenticate dispatcher)
│   ├── cleartext.rs     (async)
│   ├── md5.rs           (async send, sync hash)
│   ├── scram.rs         (async send/recv, sync crypto)
│   └── error.rs         (AuthError)
```

## Acceptance Criteria
- [ ] Trust auth works (no password exchange)
- [ ] Cleartext password auth works
- [ ] MD5 auth works
- [ ] SCRAM-SHA-256 auth works (critical path)
- [ ] Proper error on wrong password
- [ ] Server parameters collected after auth
- [ ] All I/O is async, all crypto is sync pure Rust
- [ ] Compiles to wasm32-wasip2

## Testing
- Unit test each crypto computation with known test vectors (sync)
- SCRAM test vectors from RFC 5802
- Async integration test: connect to PG with each auth method
- Test auth failure handling (wrong password, unsupported mechanism)
