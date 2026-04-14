// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::events::Event;
use super::file_sink::FileSink;
use super::http_sink::HttpSink;
use super::sink::MlogSink;

/// Serialization format for HTTP POST batches.
///
/// The file sink always uses JSON-SEQ (RFC 7464) — the canonical qlog streaming
/// format. This enum controls the HTTP sink's wire format only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpBatchFormat {
    /// JSON array: `[{event1}, {event2}, ...]`
    ///
    /// Default. Compatible with most HTTP ingest APIs.
    /// Content-Type: application/json
    #[default]
    JsonArray,

    /// Newline-delimited JSON: `{event1}\n{event2}\n`
    ///
    /// Widely used in log shipping (Elasticsearch, etc.).
    /// Content-Type: application/x-ndjson
    Ndjson,

    /// JSON Text Sequences (RFC 7464): `\x1e{event1}\n\x1e{event2}\n`
    ///
    /// The qlog-native streaming format (QlogFileSeq). Same format used by
    /// the file sink. Content-Type: application/qlog+json-seq
    /// (per draft-ietf-quic-qlog-main-schema §11.2)
    JsonSeq,
}

impl fmt::Display for HttpBatchFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpBatchFormat::JsonArray => write!(f, "json-array"),
            HttpBatchFormat::Ndjson => write!(f, "ndjson"),
            HttpBatchFormat::JsonSeq => write!(f, "json-seq"),
        }
    }
}

impl FromStr for HttpBatchFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json-array" | "json_array" | "jsonarray" => Ok(HttpBatchFormat::JsonArray),
            "ndjson" => Ok(HttpBatchFormat::Ndjson),
            "json-seq" | "json_seq" | "jsonseq" => Ok(HttpBatchFormat::JsonSeq),
            _ => Err(format!(
                "unknown format '{}': expected json-array, ndjson, or json-seq",
                s
            )),
        }
    }
}

impl HttpBatchFormat {
    /// The Content-Type header value for this format.
    ///
    /// The json-seq content type uses the qlog-specific IANA registration
    /// (`application/qlog+json-seq`) per draft-ietf-quic-qlog-main-schema §11.2,
    /// rather than the generic `application/json-seq` (RFC 7464).
    pub fn content_type(&self) -> &'static str {
        match self {
            HttpBatchFormat::JsonArray => "application/json",
            HttpBatchFormat::Ndjson => "application/x-ndjson",
            HttpBatchFormat::JsonSeq => "application/qlog+json-seq",
        }
    }
}

/// Configuration for mlog output sinks.
///
/// Both file and HTTP sinks can be active simultaneously.
/// If neither is configured, no mlog output is produced.
#[derive(Clone, Default)]
pub struct MlogConfig {
    /// Write mlog to a local file (JSON-SEQ format).
    pub file_path: Option<PathBuf>,

    /// POST mlog events to this HTTP endpoint.
    pub http_url: Option<String>,

    /// Custom HTTP headers for the HTTP sink (e.g. x-hdx-table, x-hdx-token).
    pub http_headers: HashMap<String, String>,

    /// Serialization format for HTTP POST batches.
    pub http_format: HttpBatchFormat,

    /// Shared reqwest::Client for HTTP sink connection pooling.
    /// Build once at relay startup via `HttpSink::build_client()` and share
    /// across all connections.
    pub http_client: Option<Arc<reqwest::Client>>,
}

impl fmt::Debug for MlogConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MlogConfig")
            .field("file_path", &self.file_path)
            .field("http_url", &self.http_url)
            .field(
                "http_headers",
                &format_args!("[{} headers, values redacted]", self.http_headers.len()),
            )
            .field("http_format", &self.http_format)
            .field(
                "http_client",
                &self.http_client.as_ref().map(|_| "<shared>"),
            )
            .finish()
    }
}

impl MlogConfig {
    /// Returns true if at least one sink is configured.
    pub fn has_sinks(&self) -> bool {
        self.file_path.is_some() || self.http_url.is_some()
    }
}

/// Composite mlog writer that dispatches events to multiple sinks.
///
/// This is the type threaded through the session layer as
/// `Option<Arc<Mutex<MlogWriter>>>`. The public interface (`add_event`,
/// `epoch_ms`, `finish`) is unchanged from the original single-file writer.
pub struct MlogWriter {
    sinks: Vec<Box<dyn MlogSink>>,
    start: Instant,
    epoch_offset_ms: f64,
}

impl MlogWriter {
    /// Create a new mlog writer from config, constructing all configured sinks.
    pub fn new(config: MlogConfig) -> io::Result<Self> {
        let mut sinks: Vec<Box<dyn MlogSink>> = Vec::new();

        // Capture the epoch offset once at startup, then use cheap Instant
        // for per-event timing. This avoids a SystemTime syscall per event
        // while still producing absolute epoch-ms timestamps.
        let start = Instant::now();
        let epoch_offset_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
            * 1000.0;

        if let Some(path) = config.file_path {
            sinks.push(Box::new(FileSink::new(path)?));
        }

        if let Some(url) = config.http_url {
            sinks.push(Box::new(HttpSink::new(
                url,
                config.http_headers.clone(),
                config.http_format,
                config.http_client.clone(),
            )));
        }

        Ok(Self {
            sinks,
            start,
            epoch_offset_ms,
        })
    }

    /// Get current time as epoch milliseconds for event timestamps.
    /// Per qlog-main-schema-13 Section 7.1, with time_format "relative_to_epoch"
    /// and epoch "1970-01-01T00:00:00.000Z", time values are absolute Unix epoch ms.
    ///
    /// Uses a cached epoch offset from startup plus cheap monotonic elapsed time,
    /// avoiding a SystemTime syscall per event.
    pub fn epoch_ms(&self) -> f64 {
        self.epoch_offset_ms + self.start.elapsed().as_secs_f64() * 1000.0
    }

    /// Add an event to all sinks. Serializes once and passes bytes to each sink.
    /// Best-effort per sink — one failing sink doesn't break others.
    pub fn add_event(&mut self, event: Event) -> io::Result<()> {
        if self.sinks.is_empty() {
            return Ok(());
        }
        let bytes = serde_json::to_vec(&event)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        for sink in &mut self.sinks {
            if let Err(e) = sink.add_event_bytes(&bytes) {
                tracing::warn!(error = %e, "mlog: sink add_event failed");
            }
        }
        Ok(())
    }

    /// Flush and finalize all sinks. Called when the connection ends.
    pub fn finish(&mut self) -> io::Result<()> {
        for sink in &mut self.sinks {
            if let Err(e) = sink.finish() {
                tracing::warn!(error = %e, "mlog: sink finish failed");
            }
        }
        Ok(())
    }
}

impl Drop for MlogWriter {
    fn drop(&mut self) {
        if let Err(e) = self.finish() {
            tracing::warn!(error = %e, "mlog: flush on drop failed");
        }
    }
}
