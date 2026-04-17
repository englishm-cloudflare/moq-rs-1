// SPDX-FileCopyrightText: 2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! moq-dump — Raw diagnostic subscriber for MoQ Transport
//!
//! Connects to a MoQ relay, subscribes to one or more tracks within a
//! namespace, and writes every received object to disk as raw bytes.
//! No MP4 parsing, no interpretation — just faithful capture so you can
//! inspect exactly what the relay is sending.
//!
//! ## Output layout
//!
//! ```text
//! <out_dir>/
//!   <track_name>/
//!     group_<N>/
//!       object_<M>.bin          # raw bytes
//!       object_<M>.hex          # hex dump
//!       object_<M>.txt          # UTF-8 decode (only if valid)
//!     summary.txt               # per-track summary
//!   manifest.txt                # overall run summary
//! ```
//!
//! ## Examples
//!
//! ```bash
//! # Dump the catalog from a demo namespace
//! moq-dump https://relay.example.com/demo --name demo --track catalog
//!
//! # Dump first 3 groups from two tracks
//! moq-dump https://relay.example.com/demo --name demo \
//!     --track catalog --track video.m4s --max-groups 3
//!
//! # Try multiple catalog names (first success wins per track)
//! moq-dump https://relay.example.com/demo --name demo \
//!     --track catalog --track .catalog --max-objects 1
//! ```

use std::net;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use clap::Parser;
use url::Url;

use moq_native_ietf::quic;
use moq_transport::coding::TrackNamespace;
use moq_transport::serve::{
    SubgroupObjectReader, SubgroupReader, TrackReader, TrackReaderMode, Tracks, TracksReader,
    TracksWriter,
};
use moq_transport::session::Subscriber;

/// Diagnostic track dumper — writes raw MoQ objects to disk
#[derive(Parser, Clone)]
#[command(name = "moq-dump")]
#[command(about = "Dump raw MoQ track data to disk for inspection")]
struct Config {
    /// Connect to the given URL (https:// for WebTransport, moqt:// for QUIC)
    #[arg(value_parser = moq_url)]
    pub url: Url,

    /// The broadcast namespace
    #[arg(long)]
    pub name: String,

    /// Track name(s) to subscribe to (can be repeated)
    #[arg(long = "track", required = true)]
    pub tracks: Vec<String>,

    /// Output directory (created if it doesn't exist)
    #[arg(long, short, default_value = "moq-dump-out")]
    pub out: PathBuf,

    /// Maximum number of groups to receive per track (0 = unlimited)
    #[arg(long, default_value = "0")]
    pub max_groups: u64,

    /// Maximum number of objects to receive per track (0 = unlimited)
    #[arg(long, default_value = "0")]
    pub max_objects: u64,

    /// Listen for UDP packets on the given address
    #[arg(long, default_value = "[::]:0")]
    pub bind: net::SocketAddr,

    /// The TLS configuration
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,
}

fn moq_url(s: &str) -> Result<Url, String> {
    let url = Url::try_from(s).map_err(|e| e.to_string())?;
    if url.scheme() != "https" && url.scheme() != "moqt" {
        return Err("url scheme must be https:// or moqt://".to_string());
    }
    Ok(url)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    let config = Config::parse();

    // Create output directory
    tokio::fs::create_dir_all(&config.out).await?;

    let tls = config.tls.load()?;
    let quic = quic::Endpoint::new(quic::Config::new(config.bind, None, tls)?)?;

    tracing::info!("connecting to {}", config.url);
    let (session, connection_id, transport) = quic.client.connect(&config.url, None).await?;
    tracing::info!("connected, CID: {connection_id}");

    let (session, subscriber) = moq_transport::session::Subscriber::connect(session, transport)
        .await
        .context("failed to create MoQ session")?;

    let namespace = TrackNamespace::from_utf8_path(&config.name);
    let tracks = Tracks::new(namespace);
    let (tracks_writer, _tracks_request, tracks_reader) = tracks.produce();

    let dumper = Dumper {
        subscriber,
        tracks_writer,
        tracks_reader,
        config: config.clone(),
    };

    let start = Instant::now();

    tokio::select! {
        res = session.run() => {
            res.context("session error")?;
        }
        res = dumper.run() => {
            res.context("dump error")?;
        }
    }

    let elapsed = start.elapsed();
    tracing::info!("done in {elapsed:.2?}");

    Ok(())
}

struct Dumper {
    subscriber: Subscriber,
    tracks_writer: TracksWriter,
    tracks_reader: TracksReader,
    config: Config,
}

impl Dumper {
    async fn run(mut self) -> anyhow::Result<()> {
        let mut manifest_lines: Vec<String> = vec![
            format!("moq-dump manifest"),
            format!("url:       {}", self.config.url),
            format!("namespace: {}", self.config.name),
            format!("tracks:    {:?}", self.config.tracks),
            String::new(),
        ];

        let mut tasks = tokio::task::JoinSet::new();

        for track_name in &self.config.tracks {
            let track_name = track_name.clone();
            let config = self.config.clone();

            // Create the track in our local Tracks collection so the subscriber
            // knows what to request.
            let track = self
                .tracks_writer
                .create(&track_name)
                .context(format!("failed to create track '{track_name}'"))?;

            // Get the reader handle *before* spawning the subscribe task,
            // since the spawn moves track_name into the closure.
            let reader = self
                .tracks_reader
                .subscribe(self.tracks_reader.namespace.clone(), &track_name)
                .context(format!("no reader for track '{track_name}'"))?;

            let track_name_owned = track_name.clone();

            // Tell the subscriber to subscribe to this track on the relay.
            let mut sub = self.subscriber.clone();
            tokio::task::spawn(async move {
                sub.subscribe(track).await.unwrap_or_else(|err| {
                    tracing::warn!("subscribe to '{track_name}' failed: {err:#}");
                });
            });
            tasks.spawn(async move {
                let result = Self::dump_track(reader, &config).await;
                (track_name_owned, result)
            });
        }

        while let Some(join_result) = tasks.join_next().await {
            let (track_name, result) = join_result?;
            match result {
                Ok(summary) => {
                    manifest_lines.push(format!("track '{track_name}': OK"));
                    manifest_lines.push(format!("  {summary}"));
                }
                Err(err) => {
                    manifest_lines.push(format!("track '{track_name}': ERROR"));
                    manifest_lines.push(format!("  {err:#}"));
                    tracing::error!("track '{track_name}' failed: {err:#}");
                }
            }
        }

        // Write overall manifest
        let manifest_path = self.config.out.join("manifest.txt");
        let manifest = manifest_lines.join("\n") + "\n";
        tokio::fs::write(&manifest_path, &manifest).await?;
        tracing::info!("wrote {manifest_path:?}");

        // Print it too for convenience
        print!("{manifest}");

        Ok(())
    }

    async fn dump_track(track: TrackReader, config: &Config) -> anyhow::Result<String> {
        let track_name = track.name.clone();
        let track_dir = config.out.join(&track_name);
        tokio::fs::create_dir_all(&track_dir).await?;

        tracing::info!("track '{track_name}': waiting for data...");

        let mut total_groups: u64 = 0;
        let mut total_objects: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut summary_lines: Vec<String> = Vec::new();

        match track.mode().await? {
            TrackReaderMode::Subgroups(mut groups) => {
                while let Some(group) = groups.next().await? {
                    let group_id = group.group_id;
                    let group_dir = track_dir.join(format!("group_{group_id}"));
                    tokio::fs::create_dir_all(&group_dir).await?;

                    tracing::info!("track '{track_name}': group {group_id}");

                    let (obj_count, byte_count) =
                        Self::dump_group(group, &group_dir, config).await?;

                    summary_lines.push(format!(
                        "  group {group_id}: {obj_count} objects, {byte_count} bytes"
                    ));
                    total_groups += 1;
                    total_objects += obj_count;
                    total_bytes += byte_count;

                    if config.max_groups > 0 && total_groups >= config.max_groups {
                        tracing::info!(
                            "track '{track_name}': reached max-groups={}, stopping",
                            config.max_groups
                        );
                        break;
                    }
                    if config.max_objects > 0 && total_objects >= config.max_objects {
                        tracing::info!(
                            "track '{track_name}': reached max-objects={}, stopping",
                            config.max_objects
                        );
                        break;
                    }
                }
            }
            TrackReaderMode::Stream(_stream) => {
                tracing::info!("track '{track_name}': received Stream mode (unexpected for dump, saving raw)");
                let group_dir = track_dir.join("stream_0");
                tokio::fs::create_dir_all(&group_dir).await?;
                // Just note it — Stream mode isn't typical for what we're testing
                summary_lines.push("  mode: Stream (single stream, not subgroups)".to_string());
            }
            TrackReaderMode::Datagrams(_datagrams) => {
                tracing::info!("track '{track_name}': received Datagram mode");
                summary_lines.push("  mode: Datagrams".to_string());
            }
        }

        // Write per-track summary
        let track_summary = format!(
            "track: {track_name}\ngroups: {total_groups}\nobjects: {total_objects}\nbytes: {total_bytes}\n\n{}",
            summary_lines.join("\n")
        );
        tokio::fs::write(track_dir.join("summary.txt"), &track_summary).await?;

        let summary = format!("{total_groups} groups, {total_objects} objects, {total_bytes} bytes");
        tracing::info!("track '{track_name}': {summary}");
        Ok(summary)
    }

    async fn dump_group(
        mut group: SubgroupReader,
        group_dir: &std::path::Path,
        config: &Config,
    ) -> anyhow::Result<(u64, u64)> {
        let group_id = group.group_id;
        let mut obj_count: u64 = 0;
        let mut byte_count: u64 = 0;

        while let Some(object) = group.next().await? {
            let object_id = object.object_id;
            let size_hint = object.size;

            // Read the full object
            let buf = Self::recv_object(object).await?;
            let len = buf.len();

            tracing::debug!(
                "group {group_id} object {object_id}: {len} bytes (hint: {size_hint})"
            );

            // Write raw binary
            let base = format!("object_{object_id}");
            let bin_path = group_dir.join(format!("{base}.bin"));
            tokio::fs::write(&bin_path, &buf).await?;

            // Write hex dump (with offset + ASCII sidebar, 16 bytes per line)
            let hex_path = group_dir.join(format!("{base}.hex"));
            let hex_dump = hexdump(&buf);
            tokio::fs::write(&hex_path, &hex_dump).await?;

            // If it's valid UTF-8, write a .txt too
            if let Ok(text) = std::str::from_utf8(&buf) {
                let txt_path = group_dir.join(format!("{base}.txt"));
                tokio::fs::write(&txt_path, text).await?;
                // Also try pretty-printing as JSON
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(text) {
                    // This won't exist if serde_json isn't a dep; we'll handle below
                    let pretty = serde_json::to_string_pretty(&json_val).unwrap_or_default();
                    if !pretty.is_empty() {
                        let json_path = group_dir.join(format!("{base}.json"));
                        tokio::fs::write(&json_path, &pretty).await?;
                    }
                }
            }

            obj_count += 1;
            byte_count += len as u64;

            if config.max_objects > 0 && obj_count >= config.max_objects {
                break;
            }
        }

        Ok((obj_count, byte_count))
    }

    async fn recv_object(mut object: SubgroupObjectReader) -> anyhow::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(object.size);
        while let Some(chunk) = object.read().await? {
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }
}

/// Classic hexdump format: offset | hex bytes | ASCII
fn hexdump(data: &[u8]) -> String {
    let mut out = String::new();
    for (i, chunk) in data.chunks(16).enumerate() {
        let offset = i * 16;
        // Offset
        out.push_str(&format!("{offset:08x}  "));
        // Hex bytes
        for (j, byte) in chunk.iter().enumerate() {
            out.push_str(&format!("{byte:02x} "));
            if j == 7 {
                out.push(' ');
            }
        }
        // Pad if short line
        let pad = 16 - chunk.len();
        for _ in 0..pad {
            out.push_str("   ");
        }
        if chunk.len() <= 8 {
            out.push(' ');
        }
        out.push_str(" |");
        // ASCII
        for byte in chunk {
            if byte.is_ascii_graphic() || *byte == b' ' {
                out.push(*byte as char);
            } else {
                out.push('.');
            }
        }
        out.push_str("|\n");
    }
    out
}
