use super::*;

#[derive(Clone)]
pub(super) struct LiveSource {
    instance: String,
    history: Arc<Mutex<Vec<RawApiEvent>>>,
    subscribers: Arc<Mutex<Vec<mpsc::SyncSender<RawApiEvent>>>>,
}

impl LiveSource {
    pub(super) fn new(instance: &str) -> Self {
        Self {
            instance: instance.into(),
            history: Arc::new(Mutex::new(Vec::new())),
            subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub(super) fn emit(&self, mut event: RawApiEvent) {
        event.core_instance_id = Some(self.instance.clone());
        self.history.lock().unwrap().push(event.clone());
        self.subscribers
            .lock()
            .unwrap()
            .retain(|tx| tx.try_send(event.clone()).is_ok());
    }
    pub(super) fn retain_after(&self, sequence: u64) {
        self.history
            .lock()
            .unwrap()
            .retain(|event| event.sequence.unwrap() > sequence);
    }
}

pub(super) struct Stream {
    initial: Mutex<Option<RawApiEvent>>,
    rx: mpsc::Receiver<RawApiEvent>,
}
impl EventStream for Stream {
    fn recv(&self) -> Option<RawApiEvent> {
        self.initial
            .lock()
            .unwrap()
            .take()
            .or_else(|| self.rx.recv().ok())
    }
    fn try_recv(&self) -> Option<RawApiEvent> {
        self.initial
            .lock()
            .unwrap()
            .take()
            .or_else(|| self.rx.try_recv().ok())
    }
    fn recv_timeout(&self, timeout: Duration) -> EventStreamReceive {
        if let Some(event) = self.initial.lock().unwrap().take() {
            return EventStreamReceive::Event(Box::new(event));
        }
        match self.rx.recv_timeout(timeout) {
            Ok(event) => EventStreamReceive::Event(Box::new(event)),
            Err(mpsc::RecvTimeoutError::Timeout) => EventStreamReceive::Timeout,
            Err(mpsc::RecvTimeoutError::Disconnected) => EventStreamReceive::Closed,
        }
    }
}
impl EventSource for LiveSource {
    type Stream = Stream;
    fn subscribe(&self, _: EventFilter) -> zero_api::ApiResult<Stream> {
        let (tx, rx) = mpsc::sync_channel(64);
        self.subscribers.lock().unwrap().push(tx);
        let watermark = self
            .history
            .lock()
            .unwrap()
            .last()
            .and_then(|e| e.sequence)
            .unwrap_or(0);
        Ok(Stream {
            initial: Mutex::new(Some(sequenced_event(
                "baseline",
                event_type::FLOW_SNAPSHOT,
                watermark,
            ))),
            rx,
        })
    }
    fn latest(&self, limit: usize, _: EventFilter) -> zero_api::ApiResult<Vec<RawApiEvent>> {
        Ok(self
            .history
            .lock()
            .unwrap()
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect())
    }
    fn since(
        &self,
        sequence: u64,
        limit: usize,
        _: EventFilter,
    ) -> zero_api::ApiResult<zero_api::EventReplay> {
        let history = self.history.lock().unwrap();
        let first = history
            .first()
            .and_then(|event| event.sequence)
            .unwrap_or(sequence + 1);
        Ok(zero_api::EventReplay {
            core_instance_id: self.instance.clone(),
            requested_after: sequence,
            actual_from: first.max(sequence + 1),
            has_gap: first > sequence + 1,
            events: history
                .iter()
                .filter(|event| event.sequence.unwrap() > sequence)
                .take(limit)
                .cloned()
                .collect(),
        })
    }
}
