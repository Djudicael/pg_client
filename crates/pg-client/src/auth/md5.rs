//! MD5 password authentication (client → server).
//!
//! Uses `postgres_protocol::authentication::md5_hash` for the hash computation.

use crate::protocol::{authentication, FrontendMessage};

use crate::auth::{AuthError, Codec};
use crate::config::Config;
use crate::transport::AsyncTransport;

/// Perform MD5 authentication.
pub async fn auth<T: AsyncTransport>(
    transport: &mut T,
    codec: &mut Codec,
    config: &Config,
    salt: [u8; 4],
) -> Result<(), AuthError> {
    let password = config.get_password().ok_or(AuthError::PasswordRequired)?;
    let hash = authentication::md5_hash(config.get_user().as_bytes(), password.as_bytes(), salt);
    codec
        .send(
            transport,
            &FrontendMessage::PasswordMessage {
                password: hash.into_bytes(),
            },
        )
        .await
}
