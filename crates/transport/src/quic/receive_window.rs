//! Shared receive-window tuning. Reference: apernet/quic-go 184d081eef3e,
//! flow_controller_{base,connection,stream}.go (MIT; see LICENSE-QUIC-GO).
use quinn_proto::{
    receive_window::{ReceiveWindowController, ReceiveWindowFactory},
    StreamId,
};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

#[derive(Debug)]
pub(super) struct Factory {
    pub stream_max: u64,
    pub connection_max: u64,
}
impl ReceiveWindowFactory for Factory {
    fn build(&self, stream: u64, connection: u64) -> Box<dyn ReceiveWindowController> {
        Box::new(Controller {
            stream_initial: stream,
            stream_max: self.stream_max,
            streams: BTreeMap::new(),
            connection: Window::new(connection, self.connection_max),
        })
    }
}

#[derive(Debug)]
struct Window {
    size: u64,
    maximum: u64,
    limit: u64,
    read: u64,
    epoch_read: u64,
    epoch: Option<Instant>,
}
impl Window {
    fn new(initial: u64, maximum: u64) -> Self {
        Self {
            size: initial,
            maximum,
            limit: initial,
            read: 0,
            epoch_read: 0,
            epoch: None,
        }
    }
    fn received(&mut self, now: Instant) {
        self.epoch.get_or_insert(now);
    }
    fn pending(&self) -> bool {
        self.limit.saturating_sub(self.read) <= (self.size as f64 * 0.75) as u64
    }
    fn update(&mut self, now: Instant, rtt: Duration) -> Option<u64> {
        if !self.pending() {
            return None;
        }
        let consumed = self.read.saturating_sub(self.epoch_read);
        if self.size != 0 && consumed > self.size / 2 && !rtt.is_zero() {
            if let Some(epoch) = self.epoch {
                let fraction = consumed as f64 / self.size as f64;
                let threshold =
                    Duration::from_nanos((4.0 * fraction * rtt.as_nanos() as f64) as u64);
                if now.saturating_duration_since(epoch) < threshold {
                    self.size = self.size.saturating_mul(2).min(self.maximum);
                }
            }
            self.epoch = Some(now);
            self.epoch_read = self.read;
        }
        self.limit = self
            .limit
            .max(self.read.saturating_add(self.size))
            .min((1 << 62) - 1);
        Some(self.limit)
    }
    fn ensure_size(&mut self, minimum: u64, now: Instant) {
        if minimum <= self.size {
            return;
        }
        self.size = minimum.min(self.maximum);
        self.epoch = Some(now);
        self.epoch_read = self.read;
    }
}

#[derive(Debug)]
struct Controller {
    stream_initial: u64,
    stream_max: u64,
    streams: BTreeMap<StreamId, Window>,
    connection: Window,
}
impl ReceiveWindowController for Controller {
    fn received(&mut self, id: StreamId, now: Instant) {
        self.connection.received(now);
        self.streams
            .entry(id)
            .or_insert_with(|| Window::new(self.stream_initial, self.stream_max))
            .received(now);
    }
    fn read_stream(&mut self, id: StreamId, bytes: u64) -> bool {
        self.streams.get_mut(&id).is_some_and(|window| {
            window.read = window.read.saturating_add(bytes);
            window.pending()
        })
    }
    fn read_connection(&mut self, bytes: u64) -> bool {
        self.connection.read = self.connection.read.saturating_add(bytes);
        self.connection.pending()
    }
    fn stream_update(&mut self, id: StreamId, now: Instant, rtt: Duration) -> Option<u64> {
        let stream = self.streams.get_mut(&id)?;
        let old = stream.size;
        let limit = stream.update(now, rtt);
        if stream.size > old {
            self.connection
                .ensure_size((stream.size as f64 * 1.5) as u64, now);
        }
        limit
    }
    fn connection_update(&mut self, now: Instant, rtt: Duration) -> Option<u64> {
        self.connection.update(now, rtt)
    }
    fn closed(&mut self, id: StreamId) {
        self.streams.remove(&id);
    }
    fn connection_window(&self) -> u64 {
        self.connection.size
    }
    fn set_connection_window(&mut self, window: u64, limit: u64) {
        // Explicit runtime overrides return the connection to a fixed window.
        self.connection.size = window;
        self.connection.maximum = window;
        self.connection.limit = limit;
        self.connection.epoch = None;
        self.connection.epoch_read = self.connection.read;
    }
}

#[cfg(test)]
#[path = "tests/receive_window.rs"]
mod tests;
