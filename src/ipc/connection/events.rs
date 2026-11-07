//! The existing subscription bridge retains each original worker until join.
use super::*;
struct QueuedFrame {
    bytes: Vec<u8>,
    _bytes: ControlBudgetPermit,
    _slot: zero_runtime::configuration::ControlConnectionPermit,
}
enum Pending {
    Initial(
        super::super::owner::EncodedEvent,
        zero_runtime::configuration::ControlConnectionPermit,
    ),
    Original(zero_engine::RetainedEvent, usize, ControlBudgetPermit),
}
pub(super) struct EventJobs {
    bridge: Option<tokio::task::JoinHandle<()>>,
    writer: Option<tokio::task::JoinHandle<io::Result<()>>>,
}
impl EventJobs {
    pub(super) fn new() -> Self {
        Self {
            bridge: None,
            writer: None,
        }
    }
    pub(super) fn start<W>(
        &mut self,
        subscriber: <ProxyHandle as EventSource>::Stream,
        mut initial: Option<(
            super::super::owner::EncodedEvent,
            zero_runtime::configuration::ControlConnectionPermit,
        )>,
        writer: Arc<TokioMutex<BufWriter<W>>>,
        stop: CancellationToken,
        budget: Arc<ControlBudget>,
        local: zero_runtime::configuration::ControlConnectionBudget,
    ) where
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        // Tokio (async) channel so the writer task can
        // `recv().await` without blocking a worker thread, and so
        // aborting it cancels at the await point immediately.
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(4096); // ceiling only; same Root local book admits live occupancy

        let cancel_for_task = stop.clone();
        let event_budget = budget.clone();
        let event_local = local.clone();

        // Drain ready events without throttling the subscription.
        // Only idle polls sleep; the bounded bridge applies backpressure
        // until the writer consumes events or drops its receiver.
        self.bridge = Some(tokio::task::spawn_blocking(move || {
            loop {
                if cancel_for_task.is_cancelled() || event_tx.is_closed() {
                    break;
                }
                let prepared = if let Some((initial, slot)) = initial.take() {
                    Some(Pending::Initial(initial, slot))
                } else if let Some(event) = subscriber.try_recv_retained() {
                    // Serialize the full ApiEvent envelope (same format
                    // as SSE) so consumers use one parsing code path
                    // for both channels.
                    let bytes = super::super::owner::serialized_len(&event)
                        .expect("event envelope is serializable")
                        + 1;
                    let Some(charge) = super::super::owner::reserve_event_bytes_blocking(
                        &event_budget,
                        bytes,
                        &cancel_for_task,
                        || event_tx.is_closed(),
                    ) else {
                        break;
                    };
                    Some(Pending::Original(event, bytes, charge))
                } else {
                    None
                };
                if let Some(prepared) = prepared {
                    let (prepared, frame_slot) = match prepared {
                        Pending::Initial(frame, slot) => (frame, slot),
                        Pending::Original(event, bytes, charge) => {
                            let frame_slot = loop {
                                if cancel_for_task.is_cancelled() || event_tx.is_closed() {
                                    return;
                                }
                                match event_local.reserve(
                            zero_runtime::configuration::ControlConnectionCharge::EventFrame,
                        ) {
                            Ok(slot) => break slot,
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                std::thread::sleep(Duration::from_millis(25))
                            }
                            Err(_) => return,
                        }
                            };
                            let frame = super::frames::serialize_measured(&event, bytes)
                                .expect("measured event envelope serialization");
                            (
                                super::super::owner::EncodedEvent {
                                    bytes: frame,
                                    credit: charge,
                                },
                                frame_slot,
                            )
                        }
                    };
                    if event_tx
                        .blocking_send(QueuedFrame {
                            bytes: prepared.bytes,
                            _bytes: prepared.credit,
                            _slot: frame_slot,
                        })
                        .is_err()
                    {
                        break;
                    }
                    continue;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }));

        // Writer task: pushes events and 30s heartbeats to the
        // client. Runs concurrently with the main read loop so
        // query/command/ping frames keep working after subscribe.
        let event_writer = writer.clone();
        let event_stop = stop.clone();
        self.writer = Some(tokio::spawn(async move {
            let work = async {
                loop {
                    match tokio::time::timeout(Duration::from_secs(30), event_rx.recv()).await {
                        Ok(Some(frame)) => {
                            let mut w = event_writer.lock().await;
                            w.write_all(&frame.bytes).await?;
                            w.flush().await?;
                        }
                        Ok(None) => break, // sender dropped
                        Err(_) => {
                            // heartbeat (SSE comment line; clients ignore)
                            let mut w = event_writer.lock().await;
                            w.write_all(b":\n").await?;
                            w.flush().await?;
                        }
                    }
                }
                io::Result::Ok(())
            };
            let result = tokio::select! {
                _ = event_stop.cancelled() => Ok(()),
                result = work => result,
            };
            event_stop.cancel();
            result
        }));
    }
    pub(super) async fn join(&mut self) -> [io::Result<()>; 2] {
        let events = match self.writer.take() {
            Some(task) => match task.await {
                Ok(Err(error)) if is_transient_disconnect(&error) => {
                    tracing::debug!(%error,"IPC event writer observed peer disconnect");
                    Ok(())
                }
                Ok(result) => result,
                Err(error) => Err(io::Error::other(error)),
            },
            None => Ok(()),
        };
        let bridge = match self.bridge.take() {
            Some(task) => task.await.map_err(io::Error::other),
            None => Ok(()),
        };
        [events, bridge]
    }
}
