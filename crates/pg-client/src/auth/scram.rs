//! SCRAM-SHA-256 authentication (RFC 5802 / 7677).
//!
//! This module delegates the crypto to `postgres_protocol::authentication::sasl::ScramSha256`,
//! which implements the full client-side state machine.

use pg_protocol::{
    authentication::sasl::{ChannelBinding, ScramSha256, SCRAM_SHA_256},
    BackendMessage, FrontendMessage,
};

use crate::auth::{format_error_fields, AuthError, Codec};
use crate::config::Config;
use crate::transport::AsyncTransport;

/// Perform SCRAM-SHA-256 authentication.
pub async fn auth<T: AsyncTransport>(
    transport: &mut T,
    codec: &mut Codec,
    config: &Config,
    mechanisms: &[String],
) -> Result<(), AuthError> {
    if !mechanisms.iter().any(|m| m == SCRAM_SHA_256) {
        return Err(AuthError::UnsupportedSaslMechanisms(mechanisms.to_vec()));
    }

    let password = config.get_password().ok_or(AuthError::PasswordRequired)?;

    let mut scram = ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());

    // Step 1: client-first → SASLInitialResponse
    codec
        .send(
            transport,
            &FrontendMessage::SaslInitialResponse {
                mechanism: SCRAM_SHA_256.to_string(),
                data: scram.message().to_vec(),
            },
        )
        .await?;

    // Step 2: server-first → client-final
    let msg = codec.read_message(transport).await?;
    let server_first = match msg {
        BackendMessage::AuthenticationSaslContinue(body) => body.data().to_vec(),
        BackendMessage::ErrorResponse(body) => {
            let msg = format_error_fields(&body)?;
            return Err(AuthError::ServerError(msg));
        }
        _ => return Err(AuthError::UnexpectedMessage),
    };

    scram
        .update(&server_first)
        .map_err(|e| AuthError::Scram(e.to_string()))?;

    codec
        .send(
            transport,
            &FrontendMessage::SaslResponse {
                data: scram.message().to_vec(),
            },
        )
        .await?;

    // Step 3: server-final → verify
    let msg = codec.read_message(transport).await?;
    let server_final = match msg {
        BackendMessage::AuthenticationSaslFinal(body) => body.data().to_vec(),
        BackendMessage::ErrorResponse(body) => {
            let msg = format_error_fields(&body)?;
            return Err(AuthError::ServerError(msg));
        }
        _ => return Err(AuthError::UnexpectedMessage),
    };

    scram
        .finish(&server_final)
        .map_err(|e| AuthError::Scram(e.to_string()))?;

    Ok(())
}
