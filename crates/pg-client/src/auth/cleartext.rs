//! Cleartext password authentication (client → server).

use pg_protocol::FrontendMessage;

use crate::auth::{AuthError, Codec};
use crate::config::Config;
use crate::transport::AsyncTransport;

/// Send the password in cleartext to the server.
pub async fn auth<T: AsyncTransport>(
    transport: &mut T,
    codec: &mut Codec,
    config: &Config,
) -> Result<(), AuthError> {
    let password = config.get_password().ok_or(AuthError::PasswordRequired)?;
    codec
        .send(
            transport,
            &FrontendMessage::PasswordMessage {
                password: password.as_bytes().to_vec(),
            },
        )
        .await
}
