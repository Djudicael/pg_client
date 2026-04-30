//! Read buffer management for streaming backend messages.
//!
//! [`MessageBuffer`] wraps a [`bytes::BytesMut`] and uses
//! `postgres_protocol::message::backend::Message::parse` to extract complete
//! messages from an incoming byte stream.

use bytes::BytesMut;
use postgres_protocol::message::backend::Message;

use crate::error::ProtocolError;

/// A buffer that accumulates raw wire bytes and yields parsed [`Message`]s.
///
/// # Usage
///
/// In an async read loop you typically do:
///
/// ```ignore
/// let mut buf = MessageBuffer::new();
/// loop {
///     if let Some(msg) = buf.next_message()? {
///         handle(msg);
///     }
///     let n = transport.read(&mut tmp).await?;
///     if n == 0 { break; }
///     buf.extend(&tmp[..n]);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct MessageBuffer {
    inner: BytesMut,
}

impl Default for MessageBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageBuffer {
    /// Create an empty buffer.
    pub fn new() -> Self {
        Self {
            inner: BytesMut::new(),
        }
    }

    /// Create a buffer with the given initial capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: BytesMut::with_capacity(cap),
        }
    }

    /// Create a buffer from an existing `BytesMut` (mainly useful in tests).
    pub fn from_bytesmut(inner: BytesMut) -> Self {
        Self { inner }
    }

    /// Append raw bytes received from the transport.
    pub fn extend(&mut self, data: &[u8]) {
        self.inner.extend_from_slice(data);
    }

    /// Try to parse the next complete backend message from the buffer.
    ///
    /// * If a complete message is present it is removed from the buffer and
    ///   returned.
    /// * If the buffer does not yet contain a full message `Ok(None)` is
    ///   returned and the bytes are left intact.
    /// * If the data is malformed an error is returned.
    pub fn next_message(&mut self) -> Result<Option<Message>, ProtocolError> {
        Message::parse(&mut self.inner).map_err(ProtocolError::from)
    }

    /// Returns the number of bytes currently buffered.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the buffer contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Clear the buffer, discarding all bytes.
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Returns a reference to the raw buffered bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.inner
    }
}
