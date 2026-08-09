use std::io;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use zero_api::RawApiEvent;

const SSE_HEADERS: &str = concat!(
    "HTTP/1.1 200 OK\r\n",
    "Content-Type: text/event-stream\r\n",
    "Cache-Control: no-cache\r\n",
    "Connection: keep-alive\r\n",
    "Access-Control-Allow-Origin: *\r\n",
    "\r\n",
);

/// Write a single SSE event to the stream.
pub async fn write_sse_event(stream: &mut TcpStream, event: &RawApiEvent) -> io::Result<()> {
    let json = serde_json::to_string(event).map_err(io::Error::other)?;

    let mut buf = String::new();
    if let Some(id) = event.sequence {
        buf.push_str(&format!("id: {id}\n"));
    }
    buf.push_str(&format!("event: {}\n", event.event_type));
    buf.push_str(&format!("data: {json}\n\n"));

    stream.write_all(buf.as_bytes()).await
}

/// Send a heartbeat comment to keep the SSE connection alive.
pub async fn write_sse_heartbeat(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(b": heartbeat\n\n").await
}

/// Blocking helper that runs the SSE event loop on a dedicated OS thread so
/// that the `std::sync::mpsc::Receiver` (which is `Send` but not `Sync`)
/// is never borrowed across an `.await` point.
///
/// The inner function polls `subscriber.try_recv()` with a short sleep,
/// writing every matching event to `writer`.  A heartbeat is sent after
/// `heartbeat_interval` of silence.  When the `shutdown` receiver fires
/// the connection is closed cleanly.
pub async fn run_sse_stream(
    stream: TcpStream,
    subscriber: zero_engine::EventSubscriber,
    catch_up: Vec<RawApiEvent>,
    heartbeat_interval: Duration,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) -> io::Result<()> {
    // Send headers first.
    let mut stream = stream;
    stream.write_all(SSE_HEADERS.as_bytes()).await?;

    // Write catch-up events (resumption after disconnect).
    for event in &catch_up {
        write_sse_event(&mut stream, event).await?;
    }

    // Offload the blocking recv loop to a dedicated OS thread so that the
    // mpsc Receiver (Send but not Sync) is never held across an await.
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let handle = tokio::task::spawn_blocking(move || {
        let mut last_event_at = std::time::Instant::now();
        loop {
            if let Some(event) = subscriber.try_recv() {
                if event_tx.send(SseItem::Event(Box::new(event))).is_err() {
                    break;
                }
                last_event_at = std::time::Instant::now();
            } else if last_event_at.elapsed() >= heartbeat_interval {
                if event_tx.send(SseItem::Heartbeat).is_err() {
                    break;
                }
                last_event_at = std::time::Instant::now();
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            item = event_rx.recv() => {
                let Some(item) = item else {
                    break;
                };
                match item {
                    SseItem::Event(event) => {
                        if write_sse_event(&mut stream, event.as_ref()).await.is_err() {
                            break;
                        }
                    }
                    SseItem::Heartbeat => {
                        if write_sse_heartbeat(&mut stream).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    }

    handle.abort();
    Ok(())
}

enum SseItem {
    Event(Box<RawApiEvent>),
    Heartbeat,
}
