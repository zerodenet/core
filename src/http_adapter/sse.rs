use std::{io, sync::Arc, time::Duration};
use tokio::io::AsyncWriteExt;
use zero_api::{EventStream, EventStreamReceive, RawApiEvent};
use zero_platform_tokio::contract::CancellationToken;
#[cfg(test)]
use zero_runtime::configuration::ControlBudgetLimits;
use zero_runtime::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};
const SSE_HEADERS: &str = concat!(
    "HTTP/1.1 200 OK\r\n",
    "Content-Type: text/event-stream\r\n",
    "Cache-Control: no-cache\r\n",
    "Connection: keep-alive\r\n",
    "Access-Control-Allow-Origin: *\r\n",
    "\r\n"
);
fn frame_len(event: &RawApiEvent) -> io::Result<usize> {
    let json = crate::ipc::owner::serialized_len(event)?;
    Ok(json
        + event.sequence.map_or(0, |id| 5 + id.to_string().len())
        + 8
        + event.event_type.len()
        + 6
        + 2)
}
fn frame(event: &RawApiEvent) -> io::Result<Vec<u8>> {
    use std::io::Write as _;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(frame_len(event)?)
        .map_err(io::Error::other)?;
    if let Some(id) = event.sequence {
        writeln!(&mut bytes, "id: {id}")?;
    }
    writeln!(&mut bytes, "event: {}", event.event_type)?;
    bytes.extend_from_slice(b"data: ");
    serde_json::to_writer(&mut bytes, event).map_err(io::Error::other)?;
    bytes.extend_from_slice(b"\n\n");
    Ok(bytes)
}
#[cfg(test)]
pub async fn run_sse_stream(
    stream: impl tokio::io::AsyncWrite + Unpin,
    subscriber: zero_engine::EventSubscriber,
    catch_up: Vec<RawApiEvent>,
    heartbeat_interval: Duration,
    shutdown: impl std::future::Future + Send,
) -> io::Result<()> {
    let budget = ControlBudget::new(ControlBudgetLimits::from_runtime(
        &zero_config::RuntimeOptionsConfig::default(),
    ));
    let _connection = budget.reserve(ControlCharge::Connection)?;
    let local = _connection.connection_budget().unwrap();
    let _subscription = budget.reserve(ControlCharge::Subscription)?;
    run_sse_stream_with_budget(
        stream,
        subscriber,
        catch_up,
        heartbeat_interval,
        shutdown,
        budget,
        local,
    )
    .await
}
/// The caller retained its subscription charge before creating the subscriber.
/// Frame bytes are charged before allocation and remain through the actual write.
pub async fn run_sse_stream_with_budget(
    mut stream: impl tokio::io::AsyncWrite + Unpin,
    subscriber: zero_engine::EventSubscriber,
    catch_up: Vec<RawApiEvent>,
    heartbeat_interval: Duration,
    shutdown: impl std::future::Future + Send,
    budget: Arc<ControlBudget>,
    local: zero_runtime::configuration::ControlConnectionBudget,
) -> io::Result<()> {
    let mut shutdown = Box::pin(shutdown);
    let stop = CancellationToken::new();
    let worker_stop = stop.clone();
    let worker_budget = budget.clone();
    let worker_local = local.clone();
    // This actual ordered catch-up operation reserves its one working frame
    // before the existing worker can queue new events. It keeps that slot
    // between catch-up frames, preventing N=1 from waiting on its own later queue.
    let mut catchup_slot = if catch_up.is_empty() {
        None
    } else {
        Some(local.reserve(zero_runtime::configuration::ControlConnectionCharge::SseFrame)?)
    };
    // Prepay the actual largest known catch-up frame before the worker may
    // fill the same Root byte book. No buffer is preallocated at this bound.
    // The ordered operation reuses this one working allowance until catch-up
    // finishes; each Vec still allocates only its actual measured frame.
    let catchup_bytes = catch_up.iter().try_fold(0usize, |maximum, event| {
        frame_len(event).map(|size| maximum.max(size))
    })?;
    let mut catchup_charge = if catchup_bytes == 0 {
        None
    } else {
        Some(loop {
            let changed = budget.capacity_changed();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match budget.reserve(ControlCharge::QueuedEventBytes(catchup_bytes)) {
                Ok(charge) => break charge,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    tokio::select! { _=&mut shutdown=>return Ok(()),_=changed=>{} }
                }
                Err(error) => return Err(error),
            }
        })
    };
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<QueuedFrame>(4096);
    let handle = tokio::task::spawn_blocking(move || {
        let mut last_event_at = std::time::Instant::now();
        while !worker_stop.is_cancelled() && !event_tx.is_closed() {
            let pending = match subscriber.recv_timeout(Duration::from_millis(25)) {
                EventStreamReceive::Event(event) => {
                    last_event_at = std::time::Instant::now();
                    Some(Some(event))
                }
                EventStreamReceive::Closed => break,
                EventStreamReceive::Timeout if last_event_at.elapsed() >= heartbeat_interval => {
                    last_event_at = std::time::Instant::now();
                    Some(None)
                }
                EventStreamReceive::Timeout => None,
            };
            let Some(pending) = pending else {
                continue;
            };
            let bytes = match &pending {
                Some(event) => frame_len(event).expect("serializable event envelope"),
                None => b": heartbeat\n\n".len(),
            };
            let Some(charge) = crate::ipc::owner::reserve_event_bytes_blocking(
                &worker_budget,
                bytes,
                &worker_stop,
                || event_tx.is_closed(),
            ) else {
                break;
            };
            let slot = loop {
                if worker_stop.is_cancelled() || event_tx.is_closed() {
                    return;
                }
                match worker_local
                    .reserve(zero_runtime::configuration::ControlConnectionCharge::SseFrame)
                {
                    Ok(slot) => break slot,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(_) => return,
                }
            };
            let bytes = match pending {
                Some(event) => frame(&event).expect("measured event envelope serialization"),
                None => b": heartbeat\n\n".to_vec(),
            };
            if event_tx
                .blocking_send(QueuedFrame {
                    bytes,
                    _charge: charge,
                    _slot: slot,
                })
                .is_err()
            {
                break;
            }
        }
    });
    let result = crate::ipc::owner::catch_work(async {
        tokio::select! { _ = &mut shutdown => return Ok(()), result = stream.write_all(SSE_HEADERS.as_bytes()) => result? }
        for event in &catch_up {
            let frame=frame(event)?;
            tokio::select! { _ = &mut shutdown => return Ok(()), result = stream.write_all(&frame) => result? }
        }
        drop(catchup_charge.take());
        drop(catchup_slot.take());
        loop {
            let item = tokio::select! { _ = &mut shutdown => break, item = event_rx.recv() => item };
            let Some(item) = item else { break; };
            tokio::select! { _ = &mut shutdown => break, result = stream.write_all(&item.bytes) => result? }
        }
        io::Result::Ok(())
    }).await;
    stop.cancel();
    event_rx.close();
    drop(event_rx);
    let joined = handle.await.map_err(io::Error::other);
    crate::ipc::owner::finish_caught(result, [joined])
}

struct QueuedFrame {
    bytes: Vec<u8>,
    _charge: ControlBudgetPermit,
    _slot: zero_runtime::configuration::ControlConnectionPermit,
}
#[cfg(test)]
mod tests;
