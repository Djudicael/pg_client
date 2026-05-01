//! PostgreSQL binary COPY format encoder.
//!
//! The binary COPY format has a specific structure:
//!
//! ```text
//! Header: "PGCOPY\n\xff\r\n\0" (15 bytes)
//!         + flags (4 bytes, usually 0)
//!         + header extension length (4 bytes, usually 0)
//! Tuples: field_count (i16) + [field_length (i32) + field_data (bytes)]*
//! Trailer: field_count = -1 (i16)
//! ```
//!
//! # Example
//! ```ignore
//! use wasi_pg_client::copy::BinaryCopyWriter;
//!
//! let mut writer = BinaryCopyWriter::new(2);
//! let header = writer.header().to_vec();
//! let row = writer.write_row(&[
//!     Some(b"42"),
//!     Some(b"hello"),
//! ]).to_vec();
//! let trailer = writer.trailer().to_vec();
//! ```

/// Writer for PostgreSQL binary COPY format.
///
/// This struct helps encode the binary COPY protocol header, individual rows,
/// and the terminating trailer. Each call to [`write_row`](Self::write_row)
/// appends to an internal buffer and returns a slice of the newly added bytes.
#[derive(Debug, Clone)]
pub struct BinaryCopyWriter {
    buf: Vec<u8>,
    column_count: i16,
    header_written: bool,
}

impl BinaryCopyWriter {
    /// Create a new binary COPY writer for the given number of columns.
    pub fn new(column_count: i16) -> Self {
        Self {
            buf: Vec::new(),
            column_count,
            header_written: false,
        }
    }

    /// Returns the number of columns this writer expects.
    pub fn column_count(&self) -> i16 {
        self.column_count
    }

    /// Generate and return the binary COPY file header.
    ///
    /// This should be called once at the start of the COPY stream and the
    /// returned bytes sent to the server before any row data.
    ///
    /// The header consists of:
    /// - 15-byte magic signature: `PGCOPY\n\xff\r\n\0`
    /// - 4-byte flags (0)
    /// - 4-byte header extension length (0)
    pub fn header(&mut self) -> &[u8] {
        if self.header_written {
            return &[];
        }
        self.buf.clear();
        // Magic signature
        self.buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        // Flags (0)
        self.buf.extend_from_slice(&0i32.to_be_bytes());
        // Header extension length (0)
        self.buf.extend_from_slice(&0i32.to_be_bytes());
        self.header_written = true;
        &self.buf
    }

    /// Encode a single row and return the bytes.
    ///
    /// `values` must have exactly `column_count` elements.
    /// `None` represents a NULL field.
    ///
    /// # Panics
    /// Panics if `values.len() != column_count`.
    pub fn write_row(&mut self, values: &[Option<&[u8]>]) -> &[u8] {
        assert_eq!(
            values.len() as i16,
            self.column_count,
            "value count must match column count"
        );
        let start = self.buf.len();
        self.buf.extend_from_slice(&self.column_count.to_be_bytes());
        for val in values {
            match val {
                Some(data) => {
                    self.buf
                        .extend_from_slice(&(data.len() as i32).to_be_bytes());
                    self.buf.extend_from_slice(data);
                }
                None => {
                    self.buf.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }
        &self.buf[start..]
    }

    /// Generate the binary COPY trailer (field_count = -1).
    ///
    /// This should be sent after all row data to signal the end of the
    /// COPY stream.
    pub fn trailer(&mut self) -> &[u8] {
        let start = self.buf.len();
        self.buf.extend_from_slice(&(-1i16).to_be_bytes());
        &self.buf[start..]
    }

    /// Consume the writer and return the complete buffer.
    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    /// Return a reference to the accumulated buffer.
    pub fn buffer(&self) -> &[u8] {
        &self.buf
    }

    /// Clear the internal buffer, keeping the allocation.
    pub fn clear(&mut self) {
        self.buf.clear();
        self.header_written = false;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_header() {
        let mut writer = BinaryCopyWriter::new(2);
        let header = writer.header();
        assert_eq!(header.len(), 11 + 4 + 4); // 11 magic + 4 flags + 4 ext len = 19
        assert_eq!(&header[..11], b"PGCOPY\n\xff\r\n\0");
        assert_eq!(&header[11..15], &[0, 0, 0, 0]); // flags = 0
        assert_eq!(&header[15..19], &[0, 0, 0, 0]); // ext len = 0
    }

    #[test]
    fn test_binary_row() {
        let mut writer = BinaryCopyWriter::new(2);
        let _header = writer.header();
        let row = writer.write_row(&[Some(b"42"), Some(b"hello")]);

        // field_count (i16) = 2
        assert_eq!(&row[..2], &[0, 2]);
        // field 1 length (i32) = 2, data = "42"
        assert_eq!(&row[2..6], &[0, 0, 0, 2]);
        assert_eq!(&row[6..8], b"42");
        // field 2 length (i32) = 5, data = "hello"
        assert_eq!(&row[8..12], &[0, 0, 0, 5]);
        assert_eq!(&row[12..17], b"hello");
    }

    #[test]
    fn test_binary_null() {
        let mut writer = BinaryCopyWriter::new(1);
        let _header = writer.header();
        let row = writer.write_row(&[None]);

        // field_count = 1
        assert_eq!(&row[..2], &[0, 1]);
        // field length = -1 (NULL)
        assert_eq!(&row[2..6], &[0xff, 0xff, 0xff, 0xff]);
    }

    #[test]
    fn test_binary_trailer() {
        let mut writer = BinaryCopyWriter::new(1);
        let trailer = writer.trailer();
        assert_eq!(trailer.len(), 2);
        assert_eq!(i16::from_be_bytes([trailer[0], trailer[1]]), -1);
    }

    #[test]
    fn test_binary_roundtrip_structure() {
        let mut writer = BinaryCopyWriter::new(3);
        let header = writer.header().to_vec();
        let row1 = writer
            .write_row(&[Some(b"1"), Some(b"alice"), None])
            .to_vec();
        let row2 = writer
            .write_row(&[Some(b"2"), Some(b"bob"), Some(b"extra")])
            .to_vec();
        let trailer = writer.trailer().to_vec();

        let mut all = Vec::new();
        all.extend_from_slice(&header);
        all.extend_from_slice(&row1);
        all.extend_from_slice(&row2);
        all.extend_from_slice(&trailer);

        // Verify header
        assert_eq!(&all[..11], b"PGCOPY\n\xff\r\n\0");

        // Verify row 1 starts at offset 19 (after 19-byte header)
        let row1_offset = 19;
        assert_eq!(
            i16::from_be_bytes([all[row1_offset], all[row1_offset + 1]]),
            3
        );

        // Verify trailer is last 2 bytes
        let last_two = &all[all.len() - 2..];
        assert_eq!(i16::from_be_bytes([last_two[0], last_two[1]]), -1);
    }

    #[test]
    #[should_panic(expected = "value count must match column count")]
    fn test_wrong_column_count_panics() {
        let mut writer = BinaryCopyWriter::new(2);
        writer.write_row(&[Some(b"only_one")]);
    }
}
