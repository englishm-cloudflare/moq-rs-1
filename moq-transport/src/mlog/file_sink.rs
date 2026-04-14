use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use super::sink::MlogSink;

/// File-based mlog sink writing JSON-SEQ format (RFC 7464).
///
/// Extracted from the original `MlogWriter` — this handles all file I/O
/// including the qlog-compatible header record.
pub struct FileSink {
    writer: BufWriter<File>,
}

impl FileSink {
    /// Create a new file sink, writing the qlog header as the first record.
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write qlog JSON-SEQ file header as first record (RFC 7464)
        // per draft-ietf-quic-qlog-main-schema-13 Section 5
        //
        // Uses epoch-relative timestamps (absolute epoch-ms) so that
        // consumers can use the time field directly as a native timestamp.
        let header = serde_json::json!({
            "file_schema": "urn:ietf:params:qlog:file:sequential",
            "serialization_format": "JSON-SEQ",
            "title": "moq-relay",
            "description": "MoQ Transport events",
            "trace": {
                "vantage_point": {
                    "type": "server"
                },
                "common_fields": {
                    "time_format": "relative_to_epoch",
                    "reference_time": {
                        "clock_type": "system",
                        "epoch": "1970-01-01T00:00:00.000Z"
                    }
                },
                "event_schemas": [
                    "urn:ietf:params:qlog:events:moqt-03"
                ]
            }
        });

        writer.write_all(b"\x1e")?;
        serde_json::to_writer(&mut writer, &header)?;
        writer.write_all(b"\n")?;
        writer.flush()?;

        Ok(Self { writer })
    }
}

impl MlogSink for FileSink {
    fn add_event_bytes(&mut self, event_json: &[u8]) -> io::Result<()> {
        self.writer.write_all(b"\x1e")?;
        self.writer.write_all(event_json)?;
        self.writer.write_all(b"\n")?;
        // Let BufWriter accumulate writes — flush happens in finish() or
        // when the buffer is full. Avoids a synchronous syscall per event.
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
