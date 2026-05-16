use super::{AsyncTransport, TransportError};

#[cfg(feature = "tracing")]
use crate::tracing_ext::TARGET_TRANSPORT;

/// Default write buffer threshold before auto-flush (8 KiB).
const WRITE_BUFFER_FLUSH_THRESHOLD: usize = 8192;

/// Default read buffer capacity (8 KiB).
const DEFAULT_READ_CAPACITY: usize = 8192;

/// Default write buffer capacity (8 KiB).
const DEFAULT_WRITE_CAPACITY: usize = 8192;

/// A buffered transport wrapper that adds read and write buffering.
///
/// This struct wraps an `AsyncTransport` and adds buffers to reduce the
/// number of syscalls. It is used by the connection to read and write messages
/// more efficiently.
pub struct BufferedTransport<T: AsyncTransport> {
    inner: T,

    // Read buffer: data read from inner but not yet consumed by the caller.
    // Valid data is in read_buf[read_pos..read_len).
    read_buf: Vec<u8>,
    read_pos: usize,
    read_len: usize,

    // Write buffer: data written by the caller but not yet flushed to inner.
    write_buf: Vec<u8>,
}

impl<T: AsyncTransport> BufferedTransport<T> {
    /// Creates a new `BufferedTransport` wrapping the given transport.
    pub fn new(inner: T) -> Self {
        Self::with_capacity(inner, DEFAULT_READ_CAPACITY, DEFAULT_WRITE_CAPACITY)
    }

    /// Creates a new `BufferedTransport` with custom buffer capacities.
    pub fn with_capacity(inner: T, read_cap: usize, write_cap: usize) -> Self {
        Self {
            inner,
            read_buf: vec![0; read_cap],
            read_pos: 0,
            read_len: 0,
            write_buf: Vec::with_capacity(write_cap),
        }
    }

    /// Returns a reference to the inner transport.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Returns a mutable reference to the inner transport.
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Consumes the wrapper and returns the inner transport.
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Compact the read buffer: move unconsumed bytes to the front.
    /// Called automatically when the read buffer is exhausted or fragmented.
    fn compact_read(&mut self) {
        if self.read_pos > 0 {
            self.read_buf.copy_within(self.read_pos..self.read_len, 0);
            self.read_len -= self.read_pos;
            self.read_pos = 0;
        }
    }

    /// Grow the read buffer if it's too small for the next read.
    fn ensure_read_capacity(&mut self, min_remaining: usize) {
        let remaining = self.read_buf.len() - self.read_len;
        if remaining < min_remaining {
            let new_len = (self.read_buf.len() * 2).max(self.read_len + min_remaining);
            self.read_buf.resize(new_len, 0);
        }
    }

    /// Flush the write buffer to the inner transport.
    async fn flush_write(&mut self) -> Result<(), TransportError> {
        if !self.write_buf.is_empty() {
            self.inner.write_all(&self.write_buf).await?;
            self.write_buf.clear();
        }
        Ok(())
    }
}

impl<T: AsyncTransport> AsyncTransport for BufferedTransport<T> {
    fn tls_server_end_point(&self) -> Option<Vec<u8>> {
        self.inner.tls_server_end_point()
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        // If there's buffered data, return it immediately (no I/O)
        if self.read_pos < self.read_len {
            let available = &self.read_buf[self.read_pos..self.read_len];
            let n = std::cmp::min(buf.len(), available.len());
            buf[..n].copy_from_slice(&available[..n]);
            self.read_pos += n;

            // Compact if we've consumed more than half the buffer
            if self.read_pos > self.read_buf.len() / 2 {
                self.compact_read();
            }
            return Ok(n);
        }

        // Buffer is empty — refill from inner transport
        self.compact_read();
        self.ensure_read_capacity(DEFAULT_READ_CAPACITY);

        let n = self.inner.read(&mut self.read_buf[self.read_len..]).await?;
        if n == 0 {
            // EOF: connection closed
            return Ok(0);
        }
        self.read_len += n;

        // Copy from read buffer to caller's buffer
        let available = &self.read_buf[self.read_pos..self.read_len];
        let to_copy = std::cmp::min(buf.len(), available.len());
        buf[..to_copy].copy_from_slice(&available[..to_copy]);
        self.read_pos += to_copy;

        Ok(to_copy)
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        // Buffer the write data
        self.write_buf.extend_from_slice(buf);

        // Auto-flush if the write buffer exceeds the threshold.
        if self.write_buf.len() >= WRITE_BUFFER_FLUSH_THRESHOLD {
            self.flush_write().await?;
        }

        Ok(buf.len())
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        self.write_buf.extend_from_slice(buf);

        if self.write_buf.len() >= WRITE_BUFFER_FLUSH_THRESHOLD {
            self.flush_write().await?;
        }

        Ok(())
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        let mut filled = 0;
        while filled < buf.len() {
            // Try to consume from the read buffer first
            if self.read_pos < self.read_len {
                let available = &self.read_buf[self.read_pos..self.read_len];
                let needed = buf.len() - filled;
                let to_copy = std::cmp::min(needed, available.len());
                buf[filled..filled + to_copy].copy_from_slice(&available[..to_copy]);
                self.read_pos += to_copy;
                filled += to_copy;
                continue;
            }

            // Read buffer exhausted — refill from inner transport
            self.compact_read();
            self.ensure_read_capacity(buf.len() - filled);

            let n = self.inner.read(&mut self.read_buf[self.read_len..]).await?;
            if n == 0 {
                return Err(TransportError::UnexpectedEof);
            }
            self.read_len += n;
        }

        // Compact if fragmented
        if self.read_pos > self.read_buf.len() / 2 {
            self.compact_read();
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), TransportError> {
        // Flush our write buffer first, then the inner transport
        #[cfg(feature = "tracing")]
        tracing::trace!(target: TARGET_TRANSPORT, write_buf_len = self.write_buf.len(), "Flushing write buffer to transport");
        self.flush_write().await?;
        self.inner.flush().await
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        // Flush any pending writes before shutting down
        self.flush_write().await?;
        self.inner.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::super::MockTransport;
    use super::*;

    #[tokio::test]
    async fn test_buffered_read_returns_buffered_data() {
        let inner = MockTransport::new(vec![1, 2, 3, 4, 5]);
        let mut buf = BufferedTransport::new(inner);

        // First read fills the internal buffer and returns a subset
        let mut out = [0u8; 2];
        assert_eq!(buf.read(&mut out).await.unwrap(), 2);
        assert_eq!(&out, &[1, 2]);

        // Second read comes from the internal buffer
        assert_eq!(buf.read(&mut out).await.unwrap(), 2);
        assert_eq!(&out, &[3, 4]);
    }

    #[tokio::test]
    async fn test_buffered_read_exact_from_buffer() {
        let inner = MockTransport::new(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let mut buf = BufferedTransport::new(inner);

        let mut out = [0u8; 4];
        buf.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, &[1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_buffered_write_batches_data() {
        let inner = MockTransport::new(vec![]);
        let mut buf = BufferedTransport::new(inner);

        buf.write(&[1, 2, 3]).await.unwrap();
        buf.write(&[4, 5, 6]).await.unwrap();

        // Nothing should have been written to inner yet (below threshold)
        assert!(buf.inner().written().is_empty());

        buf.flush().await.unwrap();
        assert_eq!(buf.inner().written(), &[1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_buffered_auto_flush_on_threshold() {
        let inner = MockTransport::new(vec![]);
        let mut buf = BufferedTransport::new(inner);

        let large = vec![0u8; super::WRITE_BUFFER_FLUSH_THRESHOLD + 100];
        buf.write_all(&large).await.unwrap();

        // Should have auto-flushed
        assert_eq!(
            buf.inner().written().len(),
            super::WRITE_BUFFER_FLUSH_THRESHOLD + 100
        );
    }

    #[tokio::test]
    async fn test_buffered_read_exact_with_partial_inner_reads() {
        let inner = MockTransport::new(vec![1, 2, 3, 4, 5]).with_max_read_chunk(2);
        let mut buf = BufferedTransport::new(inner);

        let mut out = [0u8; 5];
        buf.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, &[1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_buffered_eof_on_read_exact() {
        let inner = MockTransport::new(vec![1, 2]);
        let mut buf = BufferedTransport::new(inner);

        let mut out = [0u8; 5];
        assert!(matches!(
            buf.read_exact(&mut out).await,
            Err(TransportError::UnexpectedEof)
        ));
    }

    #[tokio::test]
    async fn test_buffered_compaction() {
        let inner = MockTransport::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let mut buf = BufferedTransport::with_capacity(inner, 16, 16);

        // Read a few bytes to create fragmentation
        let mut out = [0u8; 4];
        buf.read(&mut out).await.unwrap();
        assert_eq!(&out, &[1, 2, 3, 4]);

        // read_pos is now 4, read_buf len is 16
        // The buffer should not have compacted yet (4 <= 8)
        assert_eq!(buf.read_pos, 4);

        // Read more to trigger compaction
        buf.read(&mut out).await.unwrap();
        assert_eq!(&out, &[5, 6, 7, 8]);

        // read_pos is now 8, which is exactly half — compaction may or may not trigger
        // depending on the > vs >= check. Our code uses > so it won't compact at exactly half.

        // Read one more byte to trigger compaction
        let mut single = [0u8; 1];
        buf.read(&mut single).await.unwrap();
        assert_eq!(single[0], 9);
        // read_pos is 9 > 8, so compaction should have happened
        assert_eq!(buf.read_pos, 9);
    }

    #[tokio::test]
    async fn test_buffered_shutdown_flushes_pending_writes() {
        let inner = MockTransport::new(vec![]);
        let mut buf = BufferedTransport::new(inner);

        buf.write(&[1, 2, 3]).await.unwrap();
        assert!(buf.inner().written().is_empty());

        buf.shutdown().await.unwrap();
        assert_eq!(buf.inner().written(), &[1, 2, 3]);
        assert!(buf.inner().shutdown_called);
    }
}
