use std::io;

/// A destination for mlog events.
///
/// Events arrive as pre-serialized JSON bytes. Serialization happens once
/// in `MlogWriter::add_event`, so sinks don't duplicate work and the
/// Mutex critical section is kept short.
pub trait MlogSink: Send + 'static {
    /// Write pre-serialized event JSON bytes to this sink.
    fn add_event_bytes(&mut self, event_json: &[u8]) -> io::Result<()>;

    /// Flush and finalize. Called once when the connection ends.
    fn finish(&mut self) -> io::Result<()>;
}
