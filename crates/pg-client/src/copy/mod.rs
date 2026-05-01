//! PostgreSQL COPY protocol — bulk data import / export.
//!
//! This module provides [`CopyIn`] (client → server) and [`CopyOut`]
//! (server → client) for high-performance bulk data transfer.
//!
//! # Example — COPY IN (text)
//! ```ignore
//! let mut copy = conn.copy_in("COPY my_table FROM STDIN").await?;
//! copy.write_row(&["1", "alice"]).await?;
//! copy.write_row(&["2", "bob"]).await?;
//! let rows = copy.finish().await?;
//! ```
//!
//! # Example — COPY OUT (text)
//! ```ignore
//! let mut copy = conn.copy_out("COPY my_table TO STDOUT").await?;
//! while let Some(chunk) = copy.read_next().await? {
//!     process(&chunk);
//! }
//! ```

use fallible_iterator::FallibleIterator;
use pg_protocol::{BackendMessage, FrontendMessage, TransactionStatus};

use crate::connection::{Connection, ConnectionState};
use crate::error::{Error, PgError, PgServerError, Result};

#[cfg(feature = "tracing")]
use crate::tracing_ext::{truncate_str, TARGET_COPY};

mod binary;

pub use binary::BinaryCopyWriter;

// ---------------------------------------------------------------------------
// CSV parsing helper
// ---------------------------------------------------------------------------

/// Parse a single CSV line into fields.
///
/// Handles quoted fields (where the quote character is doubled to escape),
/// fields containing the delimiter, and fields spanning multiple characters.
fn parse_csv_line(line: &str, delimiter: char, quote: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == quote {
                // Check for escaped quote (doubled)
                if chars.peek() == Some(&quote) {
                    chars.next(); // consume the second quote
                    current.push(quote);
                } else {
                    // End of quoted field
                    in_quotes = false;
                }
            } else {
                current.push(ch);
            }
        } else if ch == delimiter {
            fields.push(std::mem::take(&mut current));
        } else if ch == quote {
            in_quotes = true;
        } else {
            current.push(ch);
        }
    }

    fields.push(current);
    fields
}

// ---------------------------------------------------------------------------
// CopyFormat
// ---------------------------------------------------------------------------

/// Supported COPY formats.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CopyFormat {
    /// Text format (tab-separated, newline-terminated rows).
    #[default]
    Text,
    /// CSV format with options.
    Csv {
        /// Field delimiter (default `,`).
        delimiter: char,
        /// String representing NULL (default empty string).
        null: String,
        /// Whether to include a header row.
        header: bool,
        /// Quote character (default `"`).
        quote: char,
        /// Escape character (default `"`).
        escape: char,
    },
    /// Binary format.
    Binary,
}

impl CopyFormat {
    /// Returns the SQL `WITH (...)` clause fragment for this format.
    pub fn to_sql_options(&self) -> String {
        match self {
            CopyFormat::Text => String::new(),
            CopyFormat::Csv {
                delimiter,
                null,
                header,
                quote,
                escape,
            } => {
                format!(
                    "WITH (FORMAT csv, DELIMITER '{}', NULL '{}', HEADER {}, QUOTE '{}', ESCAPE '{}')",
                    delimiter, null, header, quote, escape
                )
            }
            CopyFormat::Binary => "WITH (FORMAT binary)".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// CopyIn
// ---------------------------------------------------------------------------

/// A writer for a COPY IN operation.
///
/// Created via [`Connection::copy_in`]. Data is sent to the server in
/// chunks. The operation must be completed with [`finish`](Self::finish)
/// or cancelled with [`cancel`](Self::cancel).
pub struct CopyIn<'a> {
    conn: &'a mut Connection,
    format: u8,
    column_formats: Vec<u16>,
    done: bool,
}

impl<'a> CopyIn<'a> {
    /// The overall format code for this COPY operation.
    ///
    /// `0` = text, `1` = binary.
    pub fn format(&self) -> u8 {
        self.format
    }

    /// Per-column format codes.
    pub fn column_formats(&self) -> &[u16] {
        &self.column_formats
    }

    /// Send a raw chunk of COPY data.
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        #[cfg(feature = "tracing")]
        tracing::trace!(target: TARGET_COPY, chunk_len = data.len(), "COPY IN: writing data chunk");
        self.conn
            .codec
            .send(
                &mut self.conn.transport,
                &FrontendMessage::CopyData {
                    data: data.to_vec(),
                },
            )
            .await
            .map_err(Error::from)
    }

    /// Send a single text-format row.
    ///
    /// Columns are joined with `\t` and terminated with `\n`.
    pub async fn write_row(&mut self, columns: &[&str]) -> Result<()> {
        let line = columns.join("\t") + "\n";
        self.write(line.as_bytes()).await
    }

    /// Send a single CSV-format row.
    ///
    /// This uses PostgreSQL CSV format rules: fields containing the delimiter,
    /// quote character, or newline are wrapped in quotes; quotes inside quoted
    /// fields are doubled.
    ///
    /// **Note:** This method cannot represent NULL values. Use
    /// [`write_csv_row_with_null`](Self::write_csv_row_with_null) if you need
    /// NULL support.
    pub async fn write_csv_row(
        &mut self,
        columns: &[&str],
        delimiter: char,
        quote: char,
    ) -> Result<()> {
        let columns: Vec<Option<&str>> = columns.iter().map(|c| Some(*c)).collect();
        self.write_csv_row_with_null(&columns, delimiter, quote, "")
            .await
    }

    /// Send a single CSV-format row with NULL support.
    ///
    /// Like [`write_csv_row`](Self::write_csv_row), but each column is an
    /// `Option<&str>`. `None` values are written as the `null_string`
    /// parameter (typically `""` for the default PostgreSQL CSV NULL
    /// representation, which is an empty string).
    ///
    /// # Example
    /// ```ignore
    /// let mut copy = conn.copy_in("COPY users (id, name) FROM STDIN WITH (FORMAT csv)").await?;
    /// copy.write_csv_row_with_null(
    ///     &[Some("1"), None, Some("active")],
    ///     ',', '"', "",
    /// ).await?;
    /// ```
    pub async fn write_csv_row_with_null(
        &mut self,
        columns: &[Option<&str>],
        delimiter: char,
        quote: char,
        null_string: &str,
    ) -> Result<()> {
        let mut line = String::new();
        for (i, col) in columns.iter().enumerate() {
            if i > 0 {
                line.push(delimiter);
            }
            match col {
                Some(val) => {
                    let needs_quote = val.contains(delimiter)
                        || val.contains(quote)
                        || val.contains('\n')
                        || val.contains('\r');
                    if needs_quote {
                        line.push(quote);
                        for ch in val.chars() {
                            if ch == quote {
                                line.push(quote);
                            }
                            line.push(ch);
                        }
                        line.push(quote);
                    } else {
                        line.push_str(val);
                    }
                }
                None => {
                    // Write the NULL representation string
                    line.push_str(null_string);
                }
            }
        }
        line.push('\n');
        self.write(line.as_bytes()).await
    }

    /// Finish the COPY operation successfully.
    ///
    /// Sends `CopyDone` and waits for `CommandComplete` + `ReadyForQuery`.
    /// Returns the number of rows copied.
    pub async fn finish(mut self) -> Result<u64> {
        self.conn
            .codec
            .send(&mut self.conn.transport, &FrontendMessage::CopyDone)
            .await
            .map_err(Error::from)?;

        let mut rows = 0u64;
        loop {
            let msg = self
                .conn
                .codec
                .read_message(&mut self.conn.transport)
                .await?;
            if self.conn.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::CommandComplete(body) => {
                    let tag = body.tag().unwrap_or("");
                    rows = crate::query::result::CommandTag::new(tag.to_string())
                        .rows_affected()
                        .unwrap_or(0);
                }
                BackendMessage::ReadyForQuery(body) => {
                    self.conn.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    self.done = true;
                    self.conn.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.conn.read_until_ready().await?;
                    self.done = true;
                    self.conn.state = ConnectionState::Idle;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                _ => {}
            }
        }

        #[cfg(feature = "tracing")]
        tracing::info!(target: TARGET_COPY, rows = rows, "COPY IN: finished");
        Ok(rows)
    }

    /// Cancel the COPY operation.
    ///
    /// Sends `CopyFail` with the given reason and waits for the server
    /// to return to ready state.
    pub async fn cancel(mut self, reason: &str) -> Result<()> {
        self.conn
            .codec
            .send(
                &mut self.conn.transport,
                &FrontendMessage::CopyFail {
                    message: reason.to_string(),
                },
            )
            .await
            .map_err(Error::from)?;

        self.conn.read_until_ready().await?;
        self.done = true;
        self.conn.state = ConnectionState::Idle;
        Ok(())
    }
}

impl<'a> Drop for CopyIn<'a> {
    fn drop(&mut self) {
        if self.done || std::thread::panicking() {
            return;
        }
        #[cfg(feature = "tracing")]
        tracing::warn!(target: TARGET_COPY, "CopyIn dropped without finish; connection may need recovery");
        // Drop cannot be async. Attempt a best-effort synchronous
        // CopyFail message so the server can recover the connection.
        //
        // For NativeTcpTransport (blocking TCP), this will work because
        // the underlying TcpStream supports blocking I/O.
        //
        // For WASI (async-only I/O), we cannot perform I/O in Drop.
        // The connection will be left in a broken state. Users must
        // call .finish().await or .cancel().await explicitly.
        //
        // We encode the CopyFail message directly into the transport's
        // write buffer. The next flush (or the transport's Drop) will
        // attempt to send it.
        self.conn
            .cancel_copy_in_sync("CopyIn dropped without finish");
    }
}

// ---------------------------------------------------------------------------
// CopyOut
// ---------------------------------------------------------------------------

/// A reader for a COPY OUT operation.
///
/// Created via [`Connection::copy_out`]. Data is received from the server
/// in chunks.
pub struct CopyOut<'a> {
    conn: &'a mut Connection,
    format: u8,
    column_formats: Vec<u16>,
    done: bool,
}

impl<'a> CopyOut<'a> {
    /// The overall format code for this COPY operation.
    ///
    /// `0` = text, `1` = binary.
    pub fn format(&self) -> u8 {
        self.format
    }

    /// Per-column format codes.
    pub fn column_formats(&self) -> &[u16] {
        &self.column_formats
    }

    /// Read the next chunk of COPY data.
    ///
    /// Returns `None` when the server has finished sending data.
    pub async fn read_next(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            let msg = self
                .conn
                .codec
                .read_message(&mut self.conn.transport)
                .await?;
            if self.conn.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::CopyData(body) => {
                    return Ok(Some(body.data().to_vec()));
                }
                BackendMessage::CopyDone => {
                    // Continue to read CommandComplete + ReadyForQuery
                }
                BackendMessage::CommandComplete(_) => {
                    // Wait for ReadyForQuery
                }
                BackendMessage::ReadyForQuery(body) => {
                    self.conn.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    self.done = true;
                    self.conn.state = ConnectionState::Idle;
                    return Ok(None);
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.conn.read_until_ready().await?;
                    self.done = true;
                    self.conn.state = ConnectionState::Idle;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                _ => {}
            }
        }
    }

    /// Read all remaining COPY data into a single buffer.
    pub async fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        while let Some(chunk) = self.read_next().await? {
            result.extend_from_slice(&chunk);
        }
        Ok(result)
    }

    /// Process each chunk with a callback (streaming).
    pub async fn for_each<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        while let Some(chunk) = self.read_next().await? {
            f(&chunk)?;
        }
        Ok(())
    }

    /// Process each text-format row with a callback.
    ///
    /// This is a convenience method for text-format COPY OUT operations.
    /// It collects all data, splits by newlines, and calls `f` for each
    /// row's tab-separated fields.
    ///
    /// # Example
    /// ```ignore
    /// let mut copy = conn.copy_out("COPY users TO STDOUT").await?;
    /// copy.for_each_row(|fields| {
    ///     println!("id={}, name={}", fields[0], fields[1]);
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn for_each_row<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&[&str]) -> Result<()>,
    {
        let mut buffer = String::new();
        while let Some(chunk) = self.read_next().await? {
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            // Process complete lines
            while let Some(newline_pos) = buffer.find('\n') {
                let line = buffer[..newline_pos].to_string();
                buffer = buffer[newline_pos + 1..].to_string();

                // Skip empty lines (trailing newline)
                if line.is_empty() {
                    continue;
                }

                let fields: Vec<&str> = line.split('\t').collect();
                f(&fields)?;
            }
        }

        // Process any remaining data without trailing newline
        let remaining = buffer.trim_end();
        if !remaining.is_empty() {
            let fields: Vec<&str> = remaining.split('\t').collect();
            f(&fields)?;
        }

        Ok(())
    }

    /// Process each CSV-format row with a callback.
    ///
    /// This is a convenience method for CSV-format COPY OUT operations.
    /// It collects all data, splits by newlines, and parses each row
    /// as a simple CSV line (handling quoted fields with the specified
    /// delimiter and quote character).
    ///
    /// # Example
    /// ```ignore
    /// let mut copy = conn.copy_out("COPY users TO STDOUT WITH (FORMAT csv)").await?;
    /// copy.for_each_csv_row(',', '"', |fields| {
    ///     println!("id={}, name={}", fields[0], fields[1]);
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn for_each_csv_row<F>(
        &mut self,
        delimiter: char,
        quote: char,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&[String]) -> Result<()>,
    {
        let mut buffer = String::new();
        while let Some(chunk) = self.read_next().await? {
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            // Process complete lines
            while let Some(newline_pos) = buffer.find('\n') {
                let line = buffer[..newline_pos].to_string();
                buffer = buffer[newline_pos + 1..].to_string();

                // Skip empty lines (trailing newline)
                if line.is_empty() {
                    continue;
                }

                let fields = parse_csv_line(&line, delimiter, quote);
                f(&fields)?;
            }
        }

        // Process any remaining data without trailing newline
        let remaining = buffer.trim_end();
        if !remaining.is_empty() {
            let fields = parse_csv_line(remaining, delimiter, quote);
            f(&fields)?;
        }

        Ok(())
    }
}

impl<'a> Drop for CopyOut<'a> {
    fn drop(&mut self) {
        if self.done || std::thread::panicking() {
            return;
        }
        #[cfg(feature = "tracing")]
        tracing::warn!(target: TARGET_COPY, "CopyOut dropped without finish; connection may need recovery");
        // Drop cannot be async. Attempt a best-effort synchronous drain
        // so the server can recover the connection.
        //
        // For NativeTcpTransport (blocking TCP), this will work because
        // the underlying TcpStream supports blocking I/O.
        //
        // For WASI (async-only I/O), we cannot perform I/O in Drop.
        // The connection will be left in a broken state. Users must
        // consume all data via read_next() or read_all() explicitly.
        self.conn.drain_copy_out_sync();
    }
}

// ---------------------------------------------------------------------------
// Connection extensions
// ---------------------------------------------------------------------------

impl Connection {
    /// Start a COPY IN operation.
    ///
    /// The `sql` should be a `COPY ... FROM STDIN` statement.
    ///
    /// # Example
    /// ```ignore
    /// let mut copy = conn.copy_in("COPY users (id, name) FROM STDIN").await?;
    /// copy.write_row(&["1", "alice"]).await?;
    /// copy.write_row(&["2", "bob"]).await?;
    /// let rows = copy.finish().await?;
    /// ```
    pub async fn copy_in(&mut self, sql: &str) -> Result<CopyIn<'_>> {
        #[cfg(feature = "tracing")]
        tracing::info!(target: TARGET_COPY, direction = "in", sql_truncated = %truncate_str(sql, 200), "Starting COPY IN operation");
        self.transition(ConnectionState::CopyIn)?;

        self.codec
            .send(
                &mut self.transport,
                &FrontendMessage::Query { sql: sql.into() },
            )
            .await?;

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            if self.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::CopyInResponse(body) => {
                    let format = body.format();
                    let mut column_formats = Vec::new();
                    let mut iter = body.column_formats();
                    while let Some(fmt) = iter.next()? {
                        column_formats.push(fmt);
                    }
                    break Ok(CopyIn {
                        conn: self,
                        format,
                        column_formats,
                        done: false,
                    });
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.read_until_ready().await?;
                    self.state = ConnectionState::Idle;
                    break Err(PgError::Server(Box::new(server_err)));
                }
                _ => {
                    self.read_until_ready().await?;
                    self.state = ConnectionState::Idle;
                    break Err(PgError::Protocol(
                        pg_protocol::ProtocolError::ProtocolViolation(
                            "expected CopyInResponse after COPY query".into(),
                        ),
                    ));
                }
            }
        }
    }

    /// Start a COPY OUT operation.
    ///
    /// The `sql` should be a `COPY ... TO STDOUT` statement.
    ///
    /// # Example
    /// ```ignore
    /// let mut copy = conn.copy_out("COPY users TO STDOUT").await?;
    /// while let Some(chunk) = copy.read_next().await? {
    ///     println!("{}", String::from_utf8_lossy(&chunk));
    /// }
    /// ```
    pub async fn copy_out(&mut self, sql: &str) -> Result<CopyOut<'_>> {
        #[cfg(feature = "tracing")]
        tracing::info!(target: TARGET_COPY, direction = "out", sql_truncated = %truncate_str(sql, 200), "Starting COPY OUT operation");
        self.transition(ConnectionState::CopyOut)?;

        self.codec
            .send(
                &mut self.transport,
                &FrontendMessage::Query { sql: sql.into() },
            )
            .await?;

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            if self.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::CopyOutResponse(body) => {
                    let format = body.format();
                    let mut column_formats = Vec::new();
                    let mut iter = body.column_formats();
                    while let Some(fmt) = iter.next()? {
                        column_formats.push(fmt);
                    }
                    break Ok(CopyOut {
                        conn: self,
                        format,
                        column_formats,
                        done: false,
                    });
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.read_until_ready().await?;
                    self.state = ConnectionState::Idle;
                    break Err(PgError::Server(Box::new(server_err)));
                }
                _ => {
                    self.read_until_ready().await?;
                    self.state = ConnectionState::Idle;
                    break Err(PgError::Protocol(
                        pg_protocol::ProtocolError::ProtocolViolation(
                            "expected CopyOutResponse after COPY query".into(),
                        ),
                    ));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Codec, ServerParams};
    use crate::config::Config;
    use crate::connection::ConnectionState;
    use crate::transport::{BufferedTransport, ClientTransport, MockTransport, PgTransport};
    use pg_protocol::TransactionStatus;
    use std::collections::VecDeque;

    fn make_connection(read_data: Vec<u8>) -> Connection {
        let transport = PgTransport::Plain(BufferedTransport::new(ClientTransport::Mock(
            MockTransport::new(read_data),
        )));
        Connection {
            transport,
            codec: Codec::new(),
            server_params: ServerParams::default(),
            state: ConnectionState::Idle,
            config: Config::new(),
            transaction_status: TransactionStatus::Idle,
            notification_queue: VecDeque::new(),
            notice_handler: None,
            statement_counter: 0,
            needs_recovery: false,
            health: crate::reconnect::session::ConnectionHealth::new(),
            session_state: crate::reconnect::session::SessionState::new(),
        }
    }

    fn build_copy_in_response(format: u8, col_formats: &[u16]) -> Vec<u8> {
        let mut buf = vec![b'G'];
        let mut body = Vec::new();
        body.push(format);
        body.extend_from_slice(&(col_formats.len() as i16).to_be_bytes());
        for fmt in col_formats {
            body.extend_from_slice(&fmt.to_be_bytes());
        }
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    fn build_copy_out_response(format: u8, col_formats: &[u16]) -> Vec<u8> {
        let mut buf = vec![b'H'];
        let mut body = Vec::new();
        body.push(format);
        body.extend_from_slice(&(col_formats.len() as i16).to_be_bytes());
        for fmt in col_formats {
            body.extend_from_slice(&fmt.to_be_bytes());
        }
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    fn build_copy_data(data: &[u8]) -> Vec<u8> {
        let mut buf = vec![b'd'];
        let len = (data.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(data);
        buf
    }

    fn build_copy_done() -> Vec<u8> {
        vec![b'c', 0, 0, 0, 4]
    }

    fn build_command_complete_msg(tag: &str) -> Vec<u8> {
        let mut buf = vec![b'C'];
        let mut body = Vec::new();
        body.extend_from_slice(tag.as_bytes());
        body.push(0);
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    fn build_ready_for_query(status: u8) -> Vec<u8> {
        vec![b'Z', 0, 0, 0, 5, status]
    }

    fn build_error_response(msg: &str) -> Vec<u8> {
        let mut buf = vec![b'E'];
        let mut body = Vec::new();
        body.push(b'S');
        body.extend_from_slice(b"ERROR\0");
        body.push(b'M');
        body.extend_from_slice(msg.as_bytes());
        body.push(0);
        body.push(0);
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    // -----------------------------------------------------------------------
    // CopyFormat
    // -----------------------------------------------------------------------

    #[test]
    fn test_copy_format_text() {
        assert_eq!(CopyFormat::Text.to_sql_options(), "");
    }

    #[test]
    fn test_copy_format_csv() {
        let fmt = CopyFormat::Csv {
            delimiter: ',',
            null: "\\N".to_string(),
            header: true,
            quote: '"',
            escape: '"',
        };
        let sql = fmt.to_sql_options();
        assert!(sql.contains("FORMAT csv"));
        assert!(sql.contains("HEADER true"));
    }

    #[test]
    fn test_copy_format_binary() {
        assert_eq!(CopyFormat::Binary.to_sql_options(), "WITH (FORMAT binary)");
    }

    // -----------------------------------------------------------------------
    // CopyIn (mock transport)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_copy_in_success() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_copy_in_response(0, &[0, 0]));
        data.extend_from_slice(&build_command_complete_msg("COPY 2"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let mut copy = conn.copy_in("COPY users FROM STDIN").await.unwrap();
        assert_eq!(copy.format(), 0);
        assert_eq!(copy.column_formats(), &[0, 0]);

        copy.write_row(&["1", "alice"]).await.unwrap();
        copy.write_row(&["2", "bob"]).await.unwrap();

        let rows = copy.finish().await.unwrap();
        assert_eq!(rows, 2);
        assert_eq!(conn.state, ConnectionState::Idle);
    }

    #[tokio::test]
    async fn test_copy_in_cancel() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_copy_in_response(0, &[]));
        data.extend_from_slice(&build_command_complete_msg("ROLLBACK"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let copy = conn.copy_in("COPY users FROM STDIN").await.unwrap();
        copy.cancel("user requested").await.unwrap();
        assert_eq!(conn.state, ConnectionState::Idle);

        // Verify CopyFail was sent
        if let PgTransport::Plain(buf) = &conn.transport {
            let mock = buf.inner();
            if let ClientTransport::Mock(m) = mock {
                assert!(m.written().windows(1).any(|w| w[0] == b'f'));
            }
        }
    }

    #[tokio::test]
    async fn test_copy_in_error_response() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_error_response("syntax error"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn.copy_in("COPY users FROM STDIN").await;
        assert!(matches!(result, Err(Error::Server(_))));
        drop(result);
        assert_eq!(conn.state, ConnectionState::Idle);
    }

    // -----------------------------------------------------------------------
    // CopyOut (mock transport)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_copy_out_success() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_copy_out_response(0, &[0, 0]));
        data.extend_from_slice(&build_copy_data(b"1\talice\n"));
        data.extend_from_slice(&build_copy_data(b"2\tbob\n"));
        data.extend_from_slice(&build_copy_done());
        data.extend_from_slice(&build_command_complete_msg("COPY 2"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let mut copy = conn.copy_out("COPY users TO STDOUT").await.unwrap();
        assert_eq!(copy.format(), 0);
        assert_eq!(copy.column_formats(), &[0, 0]);

        let chunk1 = copy.read_next().await.unwrap().unwrap();
        assert_eq!(chunk1, b"1\talice\n");

        let chunk2 = copy.read_next().await.unwrap().unwrap();
        assert_eq!(chunk2, b"2\tbob\n");

        let done = copy.read_next().await.unwrap();
        assert!(done.is_none());
        drop(copy);
        assert_eq!(conn.state, ConnectionState::Idle);
    }

    #[tokio::test]
    async fn test_copy_out_read_all() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_copy_out_response(0, &[]));
        data.extend_from_slice(&build_copy_data(b"hello"));
        data.extend_from_slice(&build_copy_data(b" world"));
        data.extend_from_slice(&build_copy_done());
        data.extend_from_slice(&build_command_complete_msg("COPY 2"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let mut copy = conn.copy_out("COPY users TO STDOUT").await.unwrap();
        let all = copy.read_all().await.unwrap();
        assert_eq!(all, b"hello world");
    }

    #[tokio::test]
    async fn test_copy_out_for_each() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_copy_out_response(0, &[]));
        data.extend_from_slice(&build_copy_data(b"a"));
        data.extend_from_slice(&build_copy_data(b"b"));
        data.extend_from_slice(&build_copy_done());
        data.extend_from_slice(&build_command_complete_msg("COPY 2"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let mut copy = conn.copy_out("COPY users TO STDOUT").await.unwrap();
        let mut acc = Vec::new();
        copy.for_each(|chunk| {
            acc.extend_from_slice(chunk);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(acc, b"ab");
    }

    #[tokio::test]
    async fn test_copy_out_error_response() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_error_response("relation does not exist"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn.copy_out("COPY missing TO STDOUT").await;
        assert!(matches!(result, Err(Error::Server(_))));
        drop(result);
        assert_eq!(conn.state, ConnectionState::Idle);
    }

    // -----------------------------------------------------------------------
    // CSV parsing
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_csv_line_simple() {
        let fields = super::parse_csv_line("1,alice,hello", ',', '"');
        assert_eq!(fields, vec!["1", "alice", "hello"]);
    }

    #[test]
    fn test_parse_csv_line_quoted() {
        let fields = super::parse_csv_line("1,\"alice\",\"hello world\"", ',', '"');
        assert_eq!(fields, vec!["1", "alice", "hello world"]);
    }

    #[test]
    fn test_parse_csv_line_escaped_quote() {
        let fields = super::parse_csv_line("1,\"says \"\"hi\"\"\",ok", ',', '"');
        assert_eq!(fields, vec!["1", "says \"hi\"", "ok"]);
    }

    #[test]
    fn test_parse_csv_line_empty_fields() {
        let fields = super::parse_csv_line("1,,3", ',', '"');
        assert_eq!(fields, vec!["1", "", "3"]);
    }

    #[test]
    fn test_parse_csv_line_single_field() {
        let fields = super::parse_csv_line("hello", ',', '"');
        assert_eq!(fields, vec!["hello"]);
    }

    #[test]
    fn test_parse_csv_line_delimiter_in_quotes() {
        let fields = super::parse_csv_line("1,\"a, b, c\",3", ',', '"');
        assert_eq!(fields, vec!["1", "a, b, c", "3"]);
    }

    #[test]
    fn test_parse_csv_line_tab_delimiter() {
        let fields = super::parse_csv_line("1\talice\thello", '\t', '"');
        assert_eq!(fields, vec!["1", "alice", "hello"]);
    }

    // -----------------------------------------------------------------------
    // write_csv_row_with_null (mock transport)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_write_csv_row_with_null() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_copy_in_response(0, &[]));
        data.extend_from_slice(&build_command_complete_msg("COPY 3"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let mut copy = conn.copy_in("COPY t FROM STDIN").await.unwrap();

        // Row with all values
        copy.write_csv_row_with_null(&[Some("1"), Some("alice"), Some("hello")], ',', '"', "")
            .await
            .unwrap();

        // Row with NULL
        copy.write_csv_row_with_null(&[Some("2"), Some("bob"), None], ',', '"', "")
            .await
            .unwrap();

        // Row with custom NULL string
        copy.write_csv_row_with_null(&[Some("3"), None, Some("world")], ',', '"', "\\N")
            .await
            .unwrap();

        let rows = copy.finish().await.unwrap();
        assert_eq!(rows, 3);

        // Verify the written data
        if let PgTransport::Plain(buf) = &conn.transport {
            let mock = buf.inner();
            if let ClientTransport::Mock(m) = mock {
                let written = m.written();
                // Find CopyData messages (type 'd')
                let mut copy_data_parts: Vec<Vec<u8>> = Vec::new();
                let mut i = 0;
                while i < written.len() {
                    if written[i] == b'd' {
                        // CopyData message
                        let len = i32::from_be_bytes([
                            written[i + 1],
                            written[i + 2],
                            written[i + 3],
                            written[i + 4],
                        ]);
                        let data_len = len as usize - 4;
                        copy_data_parts.push(written[i + 5..i + 5 + data_len].to_vec());
                        i += 5 + data_len;
                    } else {
                        i += 1;
                    }
                }

                // Check row 1: 1,alice,hello
                assert_eq!(&copy_data_parts[0], b"1,alice,hello\n");
                // Check row 2: 2,bob, (empty string for NULL)
                assert_eq!(&copy_data_parts[1], b"2,bob,\n");
                // Check row 3: 3,\N,world (custom NULL string)
                assert_eq!(&copy_data_parts[2], b"3,\\N,world\n");
            }
        }
    }

    // -----------------------------------------------------------------------
    // CopyOut for_each_row (mock transport)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_copy_out_for_each_row() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_copy_out_response(0, &[]));
        data.extend_from_slice(&build_copy_data(b"1\talice\n2\tbob\n3\tcharlie\n"));
        data.extend_from_slice(&build_copy_done());
        data.extend_from_slice(&build_command_complete_msg("COPY 3"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let mut copy = conn.copy_out("COPY users TO STDOUT").await.unwrap();

        let mut rows: Vec<Vec<String>> = Vec::new();
        copy.for_each_row(|fields| {
            rows.push(fields.iter().map(|f| f.to_string()).collect());
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec!["1", "alice"]);
        assert_eq!(rows[1], vec!["2", "bob"]);
        assert_eq!(rows[2], vec!["3", "charlie"]);
    }

    #[tokio::test]
    async fn test_copy_out_for_each_csv_row() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_copy_out_response(0, &[]));
        data.extend_from_slice(&build_copy_data(b"1,alice\n2,\"bob\"\n3,charlie\n"));
        data.extend_from_slice(&build_copy_done());
        data.extend_from_slice(&build_command_complete_msg("COPY 3"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let mut copy = conn.copy_out("COPY users TO STDOUT").await.unwrap();

        let mut rows: Vec<Vec<String>> = Vec::new();
        copy.for_each_csv_row(',', '"', |fields| {
            rows.push(fields.to_vec());
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec!["1", "alice"]);
        assert_eq!(rows[1], vec!["2", "bob"]);
        assert_eq!(rows[2], vec!["3", "charlie"]);
    }
}
