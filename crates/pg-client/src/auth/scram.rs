//! SCRAM-SHA-256 authentication (RFC 5802 / 7677).
//!
//! This module delegates the crypto to `postgres_protocol::authentication::sasl::ScramSha256`,
//! which implements the full client-side state machine.

use crate::protocol::{
    authentication::sasl::{ChannelBinding, ScramSha256, SCRAM_SHA_256, SCRAM_SHA_256_PLUS},
    BackendMessage, FrontendMessage,
};

use crate::auth::{format_error_fields, AuthError, Codec};
use crate::config::Config;
use crate::transport::AsyncTransport;

#[cfg(feature = "tracing")]
use crate::tracing_ext::TARGET_AUTH;

struct SelectedScramMechanism {
    mechanism: &'static str,
    channel_binding: ChannelBinding,
}

fn select_scram_mechanism<T: AsyncTransport>(
    transport: &T,
    mechanisms: &[String],
) -> Result<SelectedScramMechanism, AuthError> {
    let has_scram = mechanisms.iter().any(|m| m == SCRAM_SHA_256);
    let has_scram_plus = mechanisms.iter().any(|m| m == SCRAM_SHA_256_PLUS);
    let channel_binding = transport.tls_server_end_point();

    if has_scram_plus {
        if let Some(binding) = channel_binding {
            return Ok(SelectedScramMechanism {
                mechanism: SCRAM_SHA_256_PLUS,
                channel_binding: ChannelBinding::tls_server_end_point(binding),
            });
        }
    }

    if has_scram {
        let channel_binding = if channel_binding.is_some() {
            ChannelBinding::unrequested()
        } else {
            ChannelBinding::unsupported()
        };

        return Ok(SelectedScramMechanism {
            mechanism: SCRAM_SHA_256,
            channel_binding,
        });
    }

    Err(AuthError::UnsupportedSaslMechanisms(mechanisms.to_vec()))
}

/// Perform SCRAM-SHA-256 authentication.
pub async fn auth<T: AsyncTransport>(
    transport: &mut T,
    codec: &mut Codec,
    config: &Config,
    mechanisms: &[String],
) -> Result<(), AuthError> {
    let password = config.get_password().ok_or(AuthError::PasswordRequired)?;
    let selected = select_scram_mechanism(transport, mechanisms)?;

    let mut scram = ScramSha256::new(password.as_bytes(), selected.channel_binding);

    #[cfg(feature = "tracing")]
    tracing::debug!(target: TARGET_AUTH, mechanism = selected.mechanism, "Starting SCRAM authentication");

    // Step 1: client-first → SASLInitialResponse
    let nonce = scram.message().to_vec();
    #[cfg(feature = "tracing")]
    tracing::trace!(target: TARGET_AUTH, nonce_len = nonce.len(), "SCRAM client-first message generated");
    codec
        .send(
            transport,
            &FrontendMessage::SaslInitialResponse {
                mechanism: selected.mechanism.to_string(),
                data: nonce,
            },
        )
        .await?;

    // Step 2: server-first → client-final
    let msg = codec.read_message(transport).await?;
    #[cfg(feature = "tracing")]
    tracing::trace!(target: TARGET_AUTH, "SCRAM server-first message received");
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

    let client_final = scram.message().to_vec();
    #[cfg(feature = "tracing")]
    tracing::trace!(target: TARGET_AUTH, "SCRAM client-final proof generated");
    codec
        .send(
            transport,
            &FrontendMessage::SaslResponse { data: client_final },
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

    #[cfg(feature = "tracing")]
    tracing::debug!(target: TARGET_AUTH, mechanism = selected.mechanism, "SCRAM authentication verified");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportError;

    struct BindingTransport {
        binding: Option<Vec<u8>>,
    }

    impl AsyncTransport for BindingTransport {
        fn tls_server_end_point(&self) -> Option<Vec<u8>> {
            self.binding.clone()
        }

        async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, TransportError> {
            Ok(0)
        }

        async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
            Ok(buf.len())
        }

        async fn write_all(&mut self, _buf: &[u8]) -> Result<(), TransportError> {
            Ok(())
        }

        async fn read_exact(&mut self, _buf: &mut [u8]) -> Result<(), TransportError> {
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), TransportError> {
            Ok(())
        }

        async fn shutdown(&mut self) -> Result<(), TransportError> {
            Ok(())
        }
    }

    #[test]
    fn test_select_scram_mechanism_prefers_plus_when_channel_binding_available() {
        let transport = BindingTransport {
            binding: Some(vec![1, 2, 3, 4]),
        };
        let mechanisms = vec![SCRAM_SHA_256.to_string(), SCRAM_SHA_256_PLUS.to_string()];

        let selected = select_scram_mechanism(&transport, &mechanisms).unwrap();
        assert_eq!(selected.mechanism, SCRAM_SHA_256_PLUS);
    }

    #[test]
    fn test_select_scram_mechanism_falls_back_to_plain_scram_without_binding() {
        let transport = BindingTransport { binding: None };
        let mechanisms = vec![SCRAM_SHA_256.to_string(), SCRAM_SHA_256_PLUS.to_string()];

        let selected = select_scram_mechanism(&transport, &mechanisms).unwrap();
        assert_eq!(selected.mechanism, SCRAM_SHA_256);
    }

    #[test]
    fn test_select_scram_mechanism_requires_compatible_server_mechanism() {
        let transport = BindingTransport { binding: None };
        let mechanisms = vec![SCRAM_SHA_256_PLUS.to_string()];

        let err = match select_scram_mechanism(&transport, &mechanisms) {
            Ok(_) => panic!("expected unsupported SASL mechanism error"),
            Err(err) => err,
        };
        assert!(matches!(err, AuthError::UnsupportedSaslMechanisms(_)));
    }
}
