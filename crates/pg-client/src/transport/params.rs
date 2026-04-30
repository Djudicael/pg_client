use std::time::Duration;

use super::error::TransportError;

#[derive(Debug, Clone)]
pub struct ConnectionParams {
    pub host: String,
    pub port: u16,
    pub connect_timeout: Option<Duration>,
}

impl ConnectionParams {
    pub fn validate(&self) -> Result<(), TransportError> {
        if self.host.is_empty() {
            return Err(TransportError::InvalidConfig("host is empty".into()));
        }
        if self.port == 0 {
            return Err(TransportError::InvalidConfig("port cannot be 0".into()));
        }
        Ok(())
    }
}
