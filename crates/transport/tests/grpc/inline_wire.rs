//! Transparent test-only native polling observation: same IO, bytes and wakers.
use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Instant,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
#[derive(Clone)]
pub(super) struct Event {
    pub us: u128,
    pub operation: &'static str,
    pub bytes: usize,
}
pub(super) struct Snapshot {
    pub mode: bool,
    pub side: &'static str,
    pub nodelay: bool,
    pub read_calls: u64,
    pub read_bytes: usize,
    pub write_calls: u64,
    pub write_bytes: usize,
    pub last: Vec<Event>,
}
impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("native")
            .field(&self.us)
            .field(&self.operation)
            .field(&self.bytes)
            .finish()
    }
}
impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeIO")
            .field("multi", &self.mode)
            .field("side", &self.side)
            .field("nodelay", &self.nodelay)
            .field("read_calls", &self.read_calls)
            .field("read_bytes", &self.read_bytes)
            .field("write_calls", &self.write_calls)
            .field("write_bytes", &self.write_bytes)
            .field("last_us_op_bytes", &self.last)
            .finish()
    }
}
struct Counts {
    read_calls: u64,
    read_bytes: usize,
    write_calls: u64,
    write_bytes: usize,
    last: VecDeque<Event>,
}
pub(super) struct Observation {
    start: Instant,
    side: &'static str,
    mode: bool,
    nodelay: bool,
    counts: Mutex<Counts>,
}
impl Observation {
    fn record(&self, operation: &'static str, bytes: usize) {
        let mut counts = self.counts.lock().unwrap();
        if operation == "read" {
            counts.read_calls += 1;
            counts.read_bytes += bytes;
        } else {
            counts.write_calls += 1;
            counts.write_bytes += bytes;
        }
        if counts.last.len() == 24 {
            counts.last.pop_front();
        }
        counts.last.push_back(Event {
            us: self.start.elapsed().as_micros(),
            operation,
            bytes,
        });
    }
    pub(super) fn snapshot(&self) -> Snapshot {
        let counts = self.counts.lock().unwrap();
        Snapshot {
            mode: self.mode,
            side: self.side,
            nodelay: self.nodelay,
            read_calls: counts.read_calls,
            read_bytes: counts.read_bytes,
            write_calls: counts.write_calls,
            write_bytes: counts.write_bytes,
            last: counts.last.iter().cloned().collect(),
        }
    }
}
pub(super) struct Wire {
    io: TcpStream,
    observation: Arc<Observation>,
}
impl Wire {
    pub(super) fn new(
        io: TcpStream,
        mode: bool,
        side: &'static str,
        start: Instant,
    ) -> (Self, Arc<Observation>) {
        let observation = Arc::new(Observation {
            start,
            side,
            mode,
            nodelay: io.nodelay().unwrap(),
            counts: Mutex::new(Counts {
                read_calls: 0,
                read_bytes: 0,
                write_calls: 0,
                write_bytes: 0,
                last: VecDeque::with_capacity(24),
            }),
        });
        (
            Self {
                io,
                observation: observation.clone(),
            },
            observation,
        )
    }
}
impl AsyncRead for Wire {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let outcome = Pin::new(&mut self.io).poll_read(cx, buf);
        if outcome.is_ready() {
            self.observation.record("read", buf.filled().len() - before);
        }
        outcome
    }
}
impl AsyncWrite for Wire {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let outcome = Pin::new(&mut self.io).poll_write(cx, bytes);
        if let Poll::Ready(Ok(bytes)) = &outcome {
            self.observation.record("write", *bytes);
        }
        outcome
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let outcome = Pin::new(&mut self.io).poll_write_vectored(cx, bytes);
        if let Poll::Ready(Ok(bytes)) = &outcome {
            self.observation.record("vectored", *bytes);
        }
        outcome
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
