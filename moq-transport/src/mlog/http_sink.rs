use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use super::sink::MlogSink;
use super::writer::HttpBatchFormat;

/// Default maximum number of events to buffer before flushing.
const DEFAULT_BATCH_SIZE: usize = 100;

/// Default maximum time to wait before flushing a partial batch.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(1000);

/// Maximum channel capacity. Events are dropped when the channel is full
/// (e.g. if the HTTP endpoint is slow or unreachable).
const CHANNEL_CAPACITY: usize = 10_000;

/// Maximum number of characters to log from an HTTP error response body.
const MAX_ERROR_BODY_LOG_CHARS: usize = 512;

/// HTTP-based mlog sink that batches events and POSTs them to a remote endpoint.
///
/// On construction, spawns a background tokio task. Events are sent via a
/// bounded channel (non-blocking under Mutex). The background task flushes
/// batches every [`DEFAULT_BATCH_SIZE`] events or [`DEFAULT_FLUSH_INTERVAL`],
/// whichever comes first. If the channel is full (endpoint slow/unreachable),
/// new events are dropped with a warning.
///
/// The batch serialization format is configurable via [`HttpBatchFormat`]:
/// - **json-array** (default): `[{event1}, {event2}]` — `application/json`
/// - **ndjson**: `{event1}\n{event2}\n` — `application/x-ndjson`
/// - **json-seq**: `\x1e{event1}\n\x1e{event2}\n` — `application/qlog+json-seq` (RFC 7464 + qlog §11.2)
///
/// Best-effort delivery: HTTP errors are logged via `tracing::warn!` but
/// never block the session or propagate back to callers.
pub struct HttpSink {
    tx: mpsc::Sender<HttpSinkMessage>,
    /// Track dropped events for periodic warning
    drop_count: u64,
}

enum HttpSinkMessage {
    /// Pre-serialized event bytes (JSON). Serialization happens outside the
    /// Mutex to avoid cloning the Event/JsonValue tree and to keep the
    /// critical section short.
    Event(Vec<u8>),
    Finish,
}

/// Strip credentials (userinfo) from a URL for safe logging.
///
/// Handles `https://user:pass@host/path` → `https://host/path`.
/// Falls back to returning the URL as-is if no credentials are found.
pub fn sanitize_url_for_logging(url: &str) -> String {
    // Look for "://" then check for "@" before the next "/"
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];
        let slash_pos = after_scheme.find('/').unwrap_or(after_scheme.len());
        let authority = &after_scheme[..slash_pos];
        // Use rfind to handle passwords containing '@' (RFC 3986 §3.2.1)
        if let Some(at_pos) = authority.rfind('@') {
            return format!(
                "{}{}",
                &url[..scheme_end + 3],
                &after_scheme[at_pos + 1..]
            );
        }
    }
    url.to_string()
}

impl HttpSink {
    /// Build a shared reqwest::Client with custom default headers.
    ///
    /// Call this once at relay startup and pass the result to `new()` via
    /// `MlogConfig.http_client` so that all connections share the same
    /// HTTP connection pool and TLS state.
    pub fn build_client(headers: &HashMap<String, String>) -> Result<reqwest::Client, reqwest::Error> {
        let mut default_headers = reqwest::header::HeaderMap::new();
        for (key, value) in headers {
            if let (Ok(name), Ok(val)) = (
                reqwest::header::HeaderName::from_bytes(key.as_bytes()),
                reqwest::header::HeaderValue::from_str(value),
            ) {
                default_headers.insert(name, val);
            } else {
                tracing::warn!(key = %key, "mlog http: skipping invalid header");
            }
        }

        reqwest::Client::builder()
            .default_headers(default_headers)
            .timeout(Duration::from_secs(30))
            .build()
    }

    /// Create a new HTTP sink targeting the given URL.
    ///
    /// If `client` is provided, it is shared across connections (recommended).
    /// Otherwise a new client is built from `headers`.
    pub fn new(
        url: String,
        headers: HashMap<String, String>,
        format: HttpBatchFormat,
        client: Option<Arc<reqwest::Client>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);

        // Spawn background task for batching and sending
        tokio::spawn(Self::run(url, headers, format, rx, client));

        Self { tx, drop_count: 0 }
    }

    async fn run(
        url: String,
        headers: HashMap<String, String>,
        format: HttpBatchFormat,
        mut rx: mpsc::Receiver<HttpSinkMessage>,
        shared_client: Option<Arc<reqwest::Client>>,
    ) {
        // Use shared client if provided, otherwise build one
        let client = if let Some(c) = shared_client {
            c
        } else {
            match Self::build_client(&headers) {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    tracing::error!(error = %e, "mlog http: failed to build HTTP client");
                    return;
                }
            }
        };

        let safe_url = sanitize_url_for_logging(&url);
        tracing::info!(url = %safe_url, format = %format, "mlog http sink started");

        // Buffer of pre-serialized JSON event bytes
        let mut buffer: Vec<Vec<u8>> = Vec::with_capacity(DEFAULT_BATCH_SIZE);
        let mut flush_interval = tokio::time::interval(DEFAULT_FLUSH_INTERVAL);
        // Don't tick immediately — wait for the first interval
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Consume the first (immediate) tick
        flush_interval.tick().await;

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(HttpSinkMessage::Event(bytes)) => {
                            buffer.push(bytes);
                            if buffer.len() >= DEFAULT_BATCH_SIZE {
                                Self::flush(&client, &url, &format, &mut buffer).await;
                            }
                        }
                        Some(HttpSinkMessage::Finish) | None => {
                            // Flush remaining events and shut down
                            if !buffer.is_empty() {
                                Self::flush(&client, &url, &format, &mut buffer).await;
                            }
                            tracing::debug!("mlog http sink finished");
                            return;
                        }
                    }
                }
                _ = flush_interval.tick() => {
                    if !buffer.is_empty() {
                        Self::flush(&client, &url, &format, &mut buffer).await;
                    }
                }
            }
        }
    }

    /// Assemble pre-serialized event bytes into a batch body according to the configured format.
    fn assemble_batch(format: &HttpBatchFormat, batch: &[Vec<u8>]) -> Vec<u8> {
        // Pre-compute total size to avoid reallocations.
        // Per-event overhead: JsonArray=1 (comma), Ndjson=1 (\n), JsonSeq=2 (\x1e + \n)
        let data_len: usize = batch.iter().map(|b| b.len()).sum();
        let overhead = match format {
            HttpBatchFormat::JsonArray => batch.len() + 1, // commas + [] brackets
            HttpBatchFormat::Ndjson => batch.len(),        // newlines
            HttpBatchFormat::JsonSeq => batch.len() * 2,   // \x1e + \n per event
        };
        let mut body = Vec::with_capacity(data_len + overhead);

        match format {
            HttpBatchFormat::JsonArray => {
                // Standard JSON array: [{...}, {...}]
                body.push(b'[');
                for (i, event_bytes) in batch.iter().enumerate() {
                    if i > 0 {
                        body.push(b',');
                    }
                    body.extend_from_slice(event_bytes);
                }
                body.push(b']');
            }
            HttpBatchFormat::Ndjson => {
                // Newline-delimited JSON: {event}\n{event}\n
                for event_bytes in batch {
                    body.extend_from_slice(event_bytes);
                    body.push(b'\n');
                }
            }
            HttpBatchFormat::JsonSeq => {
                // JSON Text Sequences (RFC 7464): \x1e{event}\n
                for event_bytes in batch {
                    body.push(0x1e); // Record Separator
                    body.extend_from_slice(event_bytes);
                    body.push(b'\n');
                }
            }
        }
        body
    }

    async fn flush(
        client: &Arc<reqwest::Client>,
        url: &str,
        format: &HttpBatchFormat,
        buffer: &mut Vec<Vec<u8>>,
    ) {
        let count = buffer.len();
        let body = Self::assemble_batch(format, buffer);
        buffer.clear(); // Preserves allocated capacity for the next batch

        match client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, format.content_type())
            .body(body)
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    tracing::debug!(count, "mlog http: batch sent");
                } else {
                    let status = response.status();
                    // Read error body with a size guard to avoid unbounded allocation
                    // from large error pages (e.g. 10MB HTML from a misconfigured proxy).
                    let content_len = response.content_length().unwrap_or(0);
                    let truncated = if content_len > MAX_ERROR_BODY_LOG_CHARS as u64 * 4 {
                        format!("[body too large: {} bytes]", content_len)
                    } else {
                        let body = response.text().await.unwrap_or_default();
                        body.chars().take(MAX_ERROR_BODY_LOG_CHARS).collect()
                    };
                    tracing::warn!(
                        count,
                        status = %status,
                        body = %truncated,
                        "mlog http: batch rejected"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(count, error = %e, "mlog http: failed to send batch");
            }
        }
    }
}

impl MlogSink for HttpSink {
    fn add_event_bytes(&mut self, event_json: &[u8]) -> io::Result<()> {
        // Non-blocking send via bounded channel — drops events if full.
        // Bytes arrive pre-serialized from MlogWriter, so no clone/serialization here.
        match self.tx.try_send(HttpSinkMessage::Event(event_json.to_vec())) {
            Ok(()) => {
                // If we had been dropping, report how many were lost
                if self.drop_count > 0 {
                    tracing::warn!(
                        dropped = self.drop_count,
                        "mlog http: channel backpressure resolved, events were dropped"
                    );
                    self.drop_count = 0;
                }
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.drop_count += 1;
                if self.drop_count == 1 || self.drop_count.is_power_of_two() {
                    tracing::warn!(
                        dropped = self.drop_count,
                        "mlog http: channel full, dropping events"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Background task exited — nothing to do
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        match self.tx.try_send(HttpSinkMessage::Finish) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("mlog http: channel full at finish — final batch may be lost");
            }
        }
        Ok(())
    }
}
