use std::io;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use zero_api::{EventStream, EventStreamReceive, RawApiEvent};

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

/// Stream ordered events through one bounded, retained blocking bridge.
/// Stop wakes pending writes and joins the original subscriber worker.
pub async fn run_sse_stream(
    stream: TcpStream,
    subscriber: zero_engine::EventSubscriber,
    catch_up: Vec<RawApiEvent>,
    heartbeat_interval: Duration,
    shutdown: impl std::future::Future + Send,
) -> io::Result<()> {
    let mut shutdown = Box::pin(shutdown);
    let mut stream = stream;
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(32);
    let handle = tokio::task::spawn_blocking(move || {
        let mut last_event_at = std::time::Instant::now();
        while !event_tx.is_closed() {
            let item = match subscriber.recv_timeout(Duration::from_millis(25)) {
                EventStreamReceive::Event(event) => {
                    last_event_at = std::time::Instant::now();
                    Some(SseItem::Event(event))
                }
                EventStreamReceive::Closed => break,
                EventStreamReceive::Timeout if last_event_at.elapsed() >= heartbeat_interval => {
                    last_event_at = std::time::Instant::now();
                    Some(SseItem::Heartbeat)
                }
                EventStreamReceive::Timeout => None,
            };
            if let Some(item) = item {
                if event_tx.blocking_send(item).is_err() {
                    break;
                }
            }
        }
    });
    let result = async {
        // Initial headers and replay can backpressure just like live events.
        let initial = async {
            stream.write_all(SSE_HEADERS.as_bytes()).await?;
            for event in &catch_up { write_sse_event(&mut stream, event).await?; }
            io::Result::Ok(())
        };
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            result = initial => result?,
        }

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            item = event_rx.recv() => {
                let Some(item) = item else {
                    break;
                };
                // A stalled reader must not prevent shutdown while the socket
                // and bounded bridge are both backpressured.
                let write = async {
                    match item {
                        SseItem::Event(event) => write_sse_event(&mut stream, event.as_ref()).await,
                        SseItem::Heartbeat => write_sse_heartbeat(&mut stream).await,
                    }
                };
                tokio::select! {
                    _ = &mut shutdown => break,
                    result = write => {
                        result?;
                    }
                }
            }
        }
    }

        Ok(())
    }.await;
    // Closing the actual receiver wakes blocking_send even when the bridge
    // was full. Await the original worker before dropping the connection.
    event_rx.close();
    drop(event_rx);
    let joined = handle.await.map_err(io::Error::other);
    result?;
    joined?;
    Ok(())
}

enum SseItem {
    Event(Box<RawApiEvent>),
    Heartbeat,
}

#[cfg(test)]
mod tests;
