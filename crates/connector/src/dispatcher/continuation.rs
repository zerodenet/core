//! Bounded, process-local handoff. This is not a second event journal.
use super::*;

/// Opaque state for replacing a dispatcher against the same event-source
/// instance. Durable deliveries remain in their configured outbox; only the
/// existing bounded volatile working set moves with this state.
#[derive(Clone)]
pub struct EventDispatcherContinuation {
    state: Arc<Mutex<Option<ContinuationState>>>,
}

pub(super) struct ContinuationState {
    pub core_instance_id: String,
    pub last_sequence: Option<u64>,
    pub blocked: Option<BlockedEvent>,
    pub deliveries: VecDeque<PendingDelivery>,
}

impl EventDispatcherContinuation {
    pub(super) fn new(state: ContinuationState) -> Self {
        Self {
            state: Arc::new(Mutex::new(Some(state))),
        }
    }

    pub(super) fn validate(&self, instance: &str) -> ConnectorResult<()> {
        let state = self.state.lock().expect("dispatcher continuation");
        let state = state.as_ref().ok_or_else(|| {
            ConnectorError::Config("dispatcher continuation was already consumed".into())
        })?;
        if instance.is_empty() || instance != state.core_instance_id {
            return Err(ConnectorError::Config(
                "dispatcher continuation belongs to a different or unidentified core instance"
                    .into(),
            ));
        }
        Ok(())
    }

    pub(super) fn take(&self) -> ConnectorResult<ContinuationState> {
        self.state
            .lock()
            .expect("dispatcher continuation")
            .take()
            .ok_or_else(|| {
                ConnectorError::Config("dispatcher continuation was already consumed".into())
            })
    }
}

#[derive(Clone, Copy)]
pub(super) enum ShutdownMode {
    Drain,
    Handoff,
}

/// Resume an in-process replacement. A different or unidentified event-source
/// instance is rejected rather than reusing an unrelated sequence watermark.
pub fn resume_event_dispatcher_with_network<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
    network: EventDispatcherNetwork,
    continuation: EventDispatcherContinuation,
) -> ConnectorResult<Option<EventDispatcherHandle>>
where
    S: EventSource + Send + Sync + 'static,
{
    spawn_event_dispatcher_inner(
        source,
        api,
        source_dir,
        options,
        network,
        false,
        Some(continuation),
    )
}

impl EventDispatcherHandle {
    /// Stop the old subscription and hand its bounded volatile state to a
    /// replacement. Cancelling an acknowledged-but-unconfirmed HTTP request
    /// can still cause at-least-once redelivery; event identity is preserved.
    pub async fn shutdown_for_reload(mut self) -> ConnectorResult<EventDispatcherContinuation> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(ShutdownMode::Handoff);
        }
        self.task
            .await
            .map_err(|error| ConnectorError::Config(format!("dispatcher handoff failed: {error}")))?
            .ok_or_else(|| ConnectorError::Config("dispatcher stopped without continuation".into()))
    }
}
