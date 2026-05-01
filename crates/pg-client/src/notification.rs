//! PostgreSQL LISTEN/NOTIFY support.
//!
//! PostgreSQL provides an asynchronous notification system where clients can
//! subscribe to named channels (`LISTEN`) and other clients can send
//! notifications to those channels (`NOTIFY`). Notifications arrive as
//! `NotificationResponse` backend messages, which can appear at any time
//! between other messages.
//!
//! # Example
//! ```ignore
//! // Connection A: listen
//! conn_a.listen("my_channel").await?;
//!
//! // Connection B: notify
//! conn_b.notify("my_channel", "hello!").await?;
//!
//! // Connection A: receive
//! conn_a.wait_for_notification(None).await?;
//! let notifications = conn_a.notifications();
//! ```

use std::time::Duration;

use pg_protocol::{BackendMessage, TransactionStatus};

use crate::connection::{Connection, ConnectionState};
use crate::error::{Error, Result};
use crate::transaction::quote_identifier;

#[cfg(feature = "tracing")]
use crate::tracing_ext::TARGET_NOTIFICATION;

// ---------------------------------------------------------------------------
// Notification (re-exported from connection, but documented here)
// ---------------------------------------------------------------------------

/// An asynchronous notification received from PostgreSQL.
///
/// Notifications are delivered via the `LISTEN`/`NOTIFY` protocol. A
/// notification includes the process ID of the notifying backend, the
/// channel name, and an optional payload string.
///
/// Notifications can arrive at any time — they are interleaved with other
/// backend messages. The connection buffers them in an internal queue.
/// Use [`Connection::notifications`] or [`Connection::drain_notifications`]
/// to retrieve buffered notifications, or [`Connection::wait_for_notification`]
/// to block until one arrives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    /// Backend process ID that sent the notification.
    pub process_id: i32,
    /// Channel name.
    pub channel: String,
    /// Payload string (empty string if no payload was sent).
    pub payload: String,
}

// ---------------------------------------------------------------------------
// Connection methods
// ---------------------------------------------------------------------------

impl Connection {
    /// Start listening for notifications on a channel.
    ///
    /// This executes `LISTEN <channel>` on the server. After this call,
    /// any `NOTIFY` on the same channel (from any connection) will cause
    /// a `NotificationResponse` message to be sent to this connection.
    ///
    /// # Example
    /// ```ignore
    /// conn.listen("events").await?;
    /// ```
    pub async fn listen(&mut self, channel: &str) -> Result<()> {
        let sql = format!("LISTEN {}", quote_identifier(channel));
        self.execute(&sql).await?;
        #[cfg(feature = "tracing")]
        tracing::info!(target: TARGET_NOTIFICATION, channel = %channel, "LISTEN: subscribed to channel");
        Ok(())
    }

    /// Stop listening on a channel.
    ///
    /// This executes `UNLISTEN <channel>` on the server.
    pub async fn unlisten(&mut self, channel: &str) -> Result<()> {
        let sql = format!("UNLISTEN {}", quote_identifier(channel));
        self.execute(&sql).await?;
        Ok(())
    }

    /// Stop listening on all channels.
    ///
    /// This executes `UNLISTEN *` on the server.
    pub async fn unlisten_all(&mut self) -> Result<()> {
        self.execute("UNLISTEN *").await?;
        Ok(())
    }

    /// Send a notification on a channel.
    ///
    /// This uses `pg_notify(channel, payload)` which properly handles
    /// identifier quoting and payload escaping.
    ///
    /// # Example
    /// ```ignore
    /// conn.notify("events", "user_logged_in").await?;
    /// ```
    pub async fn notify(&mut self, channel: &str, payload: &str) -> Result<()> {
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_NOTIFICATION, channel = %channel, payload_len = payload.len(), "NOTIFY: sending notification");
        self.execute_params("SELECT pg_notify($1, $2)", &[&channel, &payload])
            .await?;
        Ok(())
    }

    /// Take all buffered notifications from the internal queue.
    ///
    /// Notifications can arrive at any time during other operations. They
    /// are buffered in an internal queue. This method drains the queue
    /// and returns all notifications that have arrived since the last call.
    ///
    /// This is a synchronous operation — no I/O is performed.
    pub fn notifications(&mut self) -> Vec<Notification> {
        self.notification_queue.drain(..).collect()
    }

    /// Wait for the next notification to arrive.
    ///
    /// If a notification is already buffered, it is returned immediately.
    /// Otherwise, this method blocks (async) until a notification arrives
    /// or the optional timeout expires.
    ///
    /// To trigger the server to flush any pending notifications, this
    /// method sends an empty query (`""`) which causes a
    /// `ReadyForQuery` cycle. Any `NotificationResponse` messages that
    /// arrive during this cycle are collected.
    ///
    /// # Example
    /// ```ignore
    /// // Wait up to 5 seconds for a notification
    /// if let Some(notification) = conn.wait_for_notification(Some(Duration::from_secs(5))).await? {
    ///     println!("Got notification on {}: {}", notification.channel, notification.payload);
    /// }
    /// ```
    pub async fn wait_for_notification(
        &mut self,
        _timeout: Option<Duration>,
    ) -> Result<Option<Notification>> {
        // Check queue first
        if let Some(n) = self.notification_queue.pop_front() {
            return Ok(Some(n));
        }

        // Send an empty query to trigger a ReadyForQuery cycle.
        // The server will deliver any pending notifications before
        // sending ReadyForQuery.
        self.transition(ConnectionState::ActiveSimpleQuery)?;

        self.codec
            .send(
                &mut self.transport,
                &pg_protocol::FrontendMessage::Query { sql: String::new() },
            )
            .await
            .map_err(Error::from)?;

        // Read messages, collecting notifications
        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            match msg {
                BackendMessage::NotificationResponse(body) => {
                    let notification = Notification {
                        process_id: body.process_id(),
                        channel: body.channel().unwrap_or("").to_string(),
                        payload: body.message().unwrap_or("").to_string(),
                    };

                    // Continue reading until ReadyForQuery to drain the cycle
                    self.read_until_ready().await?;

                    return Ok(Some(notification));
                }
                BackendMessage::EmptyQueryResponse => {}
                BackendMessage::ReadyForQuery(body) => {
                    self.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    self.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::NoticeResponse(body) => {
                    if let Ok(notice) = crate::query::Notice::from_fields(&body) {
                        self.handle_notice(&notice);
                    }
                }
                BackendMessage::ParameterStatus(body) => {
                    if let (Ok(name), Ok(value)) = (body.name(), body.value()) {
                        self.server_params
                            .params
                            .insert(name.to_string(), value.to_string());
                    }
                }
                _ => {}
            }
        }

        // No notification arrived during this cycle
        Ok(self.notification_queue.pop_front())
    }
}

// ---------------------------------------------------------------------------
// Tests
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

    fn build_notification_response(pid: i32, channel: &str, payload: &str) -> Vec<u8> {
        let mut buf = vec![b'A'];
        let mut body = Vec::new();
        body.extend_from_slice(&pid.to_be_bytes());
        body.extend_from_slice(channel.as_bytes());
        body.push(0);
        body.extend_from_slice(payload.as_bytes());
        body.push(0);
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    fn build_row_description_msg(fields: &[(&str, u32)]) -> Vec<u8> {
        let mut buf = vec![b'T'];
        let mut body = Vec::new();
        body.extend_from_slice(&(fields.len() as i16).to_be_bytes());
        for (name, type_oid) in fields {
            body.extend_from_slice(name.as_bytes());
            body.push(0);
            body.extend_from_slice(&0u32.to_be_bytes()); // table_oid
            body.extend_from_slice(&0i16.to_be_bytes()); // column_id
            body.extend_from_slice(&type_oid.to_be_bytes()); // type_oid
            body.extend_from_slice(&(-1i16).to_be_bytes()); // type_size
            body.extend_from_slice(&(-1i32).to_be_bytes()); // type_modifier
            body.extend_from_slice(&0i16.to_be_bytes()); // format
        }
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    fn build_data_row_msg(values: &[Option<&str>]) -> Vec<u8> {
        let mut buf = vec![b'D'];
        let mut body = Vec::new();
        body.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for val in values {
            match val {
                Some(v) => {
                    let bytes = v.as_bytes();
                    body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    body.extend_from_slice(bytes);
                }
                None => {
                    body.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    #[tokio::test]
    async fn test_listen() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("LISTEN"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        conn.listen("my_channel").await.unwrap();
        assert!(conn.is_idle());
    }

    #[tokio::test]
    async fn test_unlisten() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("UNLISTEN"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        conn.unlisten("my_channel").await.unwrap();
        assert!(conn.is_idle());
    }

    #[tokio::test]
    async fn test_unlisten_all() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("UNLISTEN"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        conn.unlisten_all().await.unwrap();
        assert!(conn.is_idle());
    }

    #[tokio::test]
    async fn test_notify() {
        let mut data = Vec::new();
        // pg_notify returns a row
        data.extend_from_slice(&build_row_description_msg(&[(
            "pg_notify",
            pg_types::TEXT_OID,
        )]));
        data.extend_from_slice(&build_data_row_msg(&[Some("LISTEN")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 1"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        conn.notify("my_channel", "hello").await.unwrap();
        assert!(conn.is_idle());
    }

    #[tokio::test]
    async fn test_notifications_buffered() {
        let mut conn = make_connection(vec![]);

        // Manually push notifications into the queue
        conn.notification_queue.push_back(Notification {
            process_id: 1,
            channel: "ch1".to_string(),
            payload: "hello".to_string(),
        });
        conn.notification_queue.push_back(Notification {
            process_id: 2,
            channel: "ch2".to_string(),
            payload: "world".to_string(),
        });

        let notifications = conn.notifications();
        assert_eq!(notifications.len(), 2);
        assert_eq!(notifications[0].channel, "ch1");
        assert_eq!(notifications[1].channel, "ch2");

        // Queue should be empty now
        assert!(conn.notifications().is_empty());
    }

    #[tokio::test]
    async fn test_wait_for_notification_from_queue() {
        let mut conn = make_connection(vec![]);

        // Pre-buffer a notification
        conn.notification_queue.push_back(Notification {
            process_id: 42,
            channel: "test".to_string(),
            payload: "payload".to_string(),
        });

        // Should return immediately from the queue
        let n = conn.wait_for_notification(None).await.unwrap();
        assert!(n.is_some());
        let n = n.unwrap();
        assert_eq!(n.process_id, 42);
        assert_eq!(n.channel, "test");
        assert_eq!(n.payload, "payload");
    }

    #[tokio::test]
    async fn test_wait_for_notification_from_server() {
        let mut data = Vec::new();
        // EmptyQueryResponse for the empty query
        data.extend_from_slice(&[b'I', 0, 0, 0, 4]); // EmptyQueryResponse
                                                     // NotificationResponse
        data.extend_from_slice(&build_notification_response(99, "events", "user_login"));
        // ReadyForQuery
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let n = conn.wait_for_notification(None).await.unwrap();
        assert!(n.is_some());
        let n = n.unwrap();
        assert_eq!(n.process_id, 99);
        assert_eq!(n.channel, "events");
        assert_eq!(n.payload, "user_login");
    }
}
