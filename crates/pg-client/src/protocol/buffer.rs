use bytes::BytesMut;
use postgres_protocol::message::backend::Message;

use super::error::ProtocolError;

pub const DEFAULT_MAX_BUFFERED_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct MessageBuffer {
    inner: BytesMut,
    max_buffered_bytes: usize,
}

impl Default for MessageBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageBuffer {
    pub fn new() -> Self {
        Self {
            inner: BytesMut::new(),
            max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: BytesMut::with_capacity(cap),
            max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
        }
    }

    pub fn from_bytesmut(inner: BytesMut) -> Self {
        Self {
            inner,
            max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
        }
    }

    pub fn with_max_buffered_bytes(max_buffered_bytes: usize) -> Self {
        Self {
            inner: BytesMut::new(),
            max_buffered_bytes,
        }
    }

    pub fn set_max_buffered_bytes(&mut self, max_buffered_bytes: usize) {
        self.max_buffered_bytes = max_buffered_bytes;
    }

    pub fn extend(&mut self, data: &[u8]) {
        self.inner.extend_from_slice(data);
    }

    pub fn try_extend(&mut self, data: &[u8]) -> Result<(), ProtocolError> {
        let actual = self.inner.len().saturating_add(data.len());
        if actual > self.max_buffered_bytes {
            return Err(ProtocolError::BufferLimitExceeded {
                limit: self.max_buffered_bytes,
                actual,
            });
        }
        self.inner.extend_from_slice(data);
        Ok(())
    }

    pub fn next_message(&mut self) -> Result<Option<Message>, ProtocolError> {
        if self.inner.len() > self.max_buffered_bytes {
            return Err(ProtocolError::BufferLimitExceeded {
                limit: self.max_buffered_bytes,
                actual: self.inner.len(),
            });
        }
        Message::parse(&mut self.inner).map_err(ProtocolError::from)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.inner
    }
}
