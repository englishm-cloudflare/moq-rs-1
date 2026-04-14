// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! MoQ Transport logging (mlog) following qlog patterns
//!
//! Based on draft-pardue-moq-qlog-moq-events but adapted for MoQ Transport draft-14
//! This creates qlog-compatible JSON-SEQ files that can be aggregated with QUIC qlog files
//!
//! ## Architecture
//!
//! Events are dispatched to one or more **sinks** via a composite `MlogWriter`.
//! Currently supported sinks:
//! - `FileSink` — local JSON-SEQ file (the original output format)
//! - `HttpSink` — batched HTTP POST to a remote endpoint

mod sink;
mod file_sink;
mod http_sink;
mod writer;

pub use http_sink::{sanitize_url_for_logging, HttpSink};
pub use writer::{HttpBatchFormat, MlogConfig, MlogWriter};

/// Re-export of the HTTP client type used by the mlog HTTP sink.
/// Allows relay crates to reference the client type without depending on reqwest directly.
pub type HttpClient = reqwest::Client;

pub mod events;
pub use events::{
    client_setup_parsed, loglevel_event, object_datagram_created, object_datagram_parsed,
    server_setup_created, subgroup_header_created, subgroup_header_parsed, subgroup_object_created,
    subgroup_object_ext_created, subgroup_object_ext_parsed, subgroup_object_parsed, Event,
    EventData, LogLevel,
};
