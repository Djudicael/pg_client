//! Session state tracking for reconnection.
//!
//! This module defines `SessionState` which tracks PostgreSQL session state
//! that would be lost on reconnection (prepared statements, LISTEN channels,
//! temporary tables, custom GUCs). It also defines `ConnectionHealth` which
//! tracks the health and reconnection history of a connection.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Session state that is lost when a connection is closed and re-established.
///
/// Used to decide whether reconnection is safe and to rebuild state after reconnect.
#[derive(Debug, Clone, Default)]
pub struct SessionState {
    /// Prepared statements (would need to be re-prepared after reconnect).
    /// Maps statement name → SQL text.
    prepared_statements: HashMap<String, String>,

    /// Channels currently being listened on (would need to re-LISTEN).
    listen_channels: HashSet<String>,

    /// Temporary tables created in this session.
    temporary_tables: HashSet<String>,

    /// Custom GUC parameters set via SET commands.
    custom_gucs: HashMap<String, String>,

    /// Whether we're inside a transaction (reconnection mid-transaction is dangerous).
    in_transaction: bool,
}

impl SessionState {
    /// Create a new empty session state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if the session has state that would be lost on reconnection.
    pub fn has_state(&self) -> bool {
        !self.prepared_statements.is_empty()
            || !self.listen_channels.is_empty()
            || !self.temporary_tables.is_empty()
            || !self.custom_gucs.is_empty()
    }

    /// Returns true if reconnection is safe (no important state would be lost).
    pub fn is_reconnect_safe(&self) -> bool {
        !self.in_transaction && !self.has_state()
    }

    // -----------------------------------------------------------------------
    // Prepared statements
    // -----------------------------------------------------------------------

    /// Track that a prepared statement was created.
    pub fn track_prepare(&mut self, name: &str, sql: &str) {
        self.prepared_statements
            .insert(name.to_string(), sql.to_string());
    }

    /// Track that a prepared statement was closed.
    pub fn track_close_statement(&mut self, name: &str) {
        self.prepared_statements.remove(name);
    }

    /// Get the SQL for a prepared statement by name.
    pub fn get_statement_sql(&self, name: &str) -> Option<&str> {
        self.prepared_statements.get(name).map(|s| s.as_str())
    }

    /// Get all prepared statement names.
    pub fn prepared_statement_names(&self) -> impl Iterator<Item = &str> {
        self.prepared_statements.keys().map(|s| s.as_str())
    }

    /// Get all prepared statements (name → SQL).
    pub fn prepared_statements(&self) -> &HashMap<String, String> {
        &self.prepared_statements
    }

    // -----------------------------------------------------------------------
    // LISTEN channels
    // -----------------------------------------------------------------------

    /// Track that a LISTEN command was issued.
    pub fn track_listen(&mut self, channel: &str) {
        self.listen_channels.insert(channel.to_string());
    }

    /// Track that an UNLISTEN command was issued.
    pub fn track_unlisten(&mut self, channel: &str) {
        self.listen_channels.remove(channel);
    }

    /// Get all listened channels.
    pub fn listen_channels(&self) -> &HashSet<String> {
        &self.listen_channels
    }

    // -----------------------------------------------------------------------
    // Temporary tables
    // -----------------------------------------------------------------------

    /// Track that a temporary table was created.
    pub fn track_temp_table(&mut self, name: &str) {
        self.temporary_tables.insert(name.to_string());
    }

    /// Get all temporary table names.
    pub fn temporary_tables(&self) -> &HashSet<String> {
        &self.temporary_tables
    }

    // -----------------------------------------------------------------------
    // Custom GUCs
    // -----------------------------------------------------------------------

    /// Track that a SET command was issued.
    pub fn track_set_guc(&mut self, key: &str, value: &str) {
        self.custom_gucs.insert(key.to_string(), value.to_string());
    }

    /// Get all custom GUC parameters.
    pub fn custom_gucs(&self) -> &HashMap<String, String> {
        &self.custom_gucs
    }

    // -----------------------------------------------------------------------
    // Transaction tracking
    // -----------------------------------------------------------------------

    /// Update the in-transaction flag.
    pub fn set_in_transaction(&mut self, in_transaction: bool) {
        self.in_transaction = in_transaction;
    }

    /// Returns true if the connection is inside a transaction.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Clear all session state (e.g., after DISCARD ALL).
    pub fn clear(&mut self) {
        self.prepared_statements.clear();
        self.listen_channels.clear();
        self.temporary_tables.clear();
        self.custom_gucs.clear();
        self.in_transaction = false;
    }
}

/// Internal health and reconnection state for a connection.
#[derive(Debug)]
pub struct ConnectionHealth {
    /// Whether the connection is believed to be alive.
    /// Set to false when a transport error occurs or ping fails.
    alive: bool,

    /// Number of times this connection has been reconnected.
    reconnect_count: u32,

    /// When this connection was last confirmed alive (successful query or ping).
    last_confirmed_alive: Option<Instant>,

    /// Whether the connection needs recovery (e.g., incomplete stream consumption).
    needs_recovery: bool,
}

impl ConnectionHealth {
    /// Create a new health state for a fresh connection.
    pub fn new() -> Self {
        Self {
            alive: true,
            reconnect_count: 0,
            last_confirmed_alive: Some(Instant::now()),
            needs_recovery: false,
        }
    }

    /// Whether the connection is believed to be alive.
    pub fn is_alive(&self) -> bool {
        self.alive
    }

    /// Mark the connection as alive (after a successful query or ping).
    pub fn mark_alive(&mut self) {
        self.alive = true;
        self.last_confirmed_alive = Some(Instant::now());
    }

    /// Mark the connection as broken.
    pub fn mark_broken(&mut self) {
        self.alive = false;
    }

    /// Number of times this connection has been reconnected.
    pub fn reconnect_count(&self) -> u32 {
        self.reconnect_count
    }

    /// Increment the reconnection count.
    pub fn increment_reconnect_count(&mut self) {
        self.reconnect_count += 1;
    }

    /// When this connection was last confirmed alive.
    pub fn last_confirmed_alive(&self) -> Option<Instant> {
        self.last_confirmed_alive
    }

    /// Whether the connection needs recovery.
    pub fn needs_recovery(&self) -> bool {
        self.needs_recovery
    }

    /// Set the needs_recovery flag.
    pub fn set_needs_recovery(&mut self, needs_recovery: bool) {
        self.needs_recovery = needs_recovery;
    }

    /// Reset health state after a successful reconnection.
    pub fn reset_after_reconnect(&mut self) {
        self.alive = true;
        self.reconnect_count += 1;
        self.last_confirmed_alive = Some(Instant::now());
        self.needs_recovery = false;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_state_empty() {
        let state = SessionState::new();
        assert!(!state.has_state());
        assert!(state.is_reconnect_safe());
    }

    #[test]
    fn test_session_state_with_prepared_statement() {
        let mut state = SessionState::new();
        state.track_prepare("stmt1", "SELECT 1");
        assert!(state.has_state());
        assert!(!state.is_reconnect_safe());
        assert_eq!(state.get_statement_sql("stmt1"), Some("SELECT 1"));
    }

    #[test]
    fn test_session_state_with_listen_channel() {
        let mut state = SessionState::new();
        state.track_listen("events");
        assert!(state.has_state());
        assert!(!state.is_reconnect_safe());
        assert!(state.listen_channels().contains("events"));
    }

    #[test]
    fn test_session_state_with_temp_table() {
        let mut state = SessionState::new();
        state.track_temp_table("tmp_data");
        assert!(state.has_state());
        assert!(!state.is_reconnect_safe());
    }

    #[test]
    fn test_session_state_with_guc() {
        let mut state = SessionState::new();
        state.track_set_guc("timezone", "UTC");
        assert!(state.has_state());
        assert!(!state.is_reconnect_safe());
        assert_eq!(
            state.custom_gucs().get("timezone"),
            Some(&"UTC".to_string())
        );
    }

    #[test]
    fn test_session_state_in_transaction() {
        let mut state = SessionState::new();
        state.set_in_transaction(true);
        assert!(!state.is_reconnect_safe());
        // Even without other state, in_transaction makes it unsafe
        assert!(!state.has_state()); // has_state doesn't count in_transaction
    }

    #[test]
    fn test_session_state_unlisten() {
        let mut state = SessionState::new();
        state.track_listen("events");
        assert!(state.has_state());
        state.track_unlisten("events");
        assert!(!state.has_state());
    }

    #[test]
    fn test_session_state_close_statement() {
        let mut state = SessionState::new();
        state.track_prepare("stmt1", "SELECT 1");
        assert!(state.has_state());
        state.track_close_statement("stmt1");
        assert!(!state.has_state());
    }

    #[test]
    fn test_session_state_clear() {
        let mut state = SessionState::new();
        state.track_prepare("stmt1", "SELECT 1");
        state.track_listen("events");
        state.track_temp_table("tmp");
        state.track_set_guc("timezone", "UTC");
        state.set_in_transaction(true);
        state.clear();
        assert!(!state.has_state());
        assert!(state.is_reconnect_safe());
    }

    #[test]
    fn test_connection_health_new() {
        let health = ConnectionHealth::new();
        assert!(health.is_alive());
        assert_eq!(health.reconnect_count(), 0);
        assert!(health.last_confirmed_alive().is_some());
        assert!(!health.needs_recovery());
    }

    #[test]
    fn test_connection_health_mark_broken() {
        let mut health = ConnectionHealth::new();
        health.mark_broken();
        assert!(!health.is_alive());
    }

    #[test]
    fn test_connection_health_mark_alive() {
        let mut health = ConnectionHealth::new();
        health.mark_broken();
        health.mark_alive();
        assert!(health.is_alive());
    }

    #[test]
    fn test_connection_health_reset_after_reconnect() {
        let mut health = ConnectionHealth::new();
        health.mark_broken();
        health.set_needs_recovery(true);
        health.reset_after_reconnect();
        assert!(health.is_alive());
        assert_eq!(health.reconnect_count(), 1);
        assert!(!health.needs_recovery());
    }
}
