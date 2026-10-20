//! Evidence comes only from consumption of the original response JoinSet.
mod set;
use super::packet_path::ChainTask;
pub(crate) use set::{ResponseSetClosed, ResponseSetLease, ResponseSetOwner};
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, Weak},
};
use tokio::{
    sync::Notify,
    task::{AbortHandle, Id, JoinError, JoinSet},
};
use zero_core::Address;
use zero_engine::EngineError;

type Packet = (Address, u16, Vec<u8>, Option<u64>);
pub(crate) type NativeChainJoin = Result<(Id, ChainTask), JoinError>;
pub(crate) type ObservedChainJoin = Result<Result<Packet, Arc<EngineError>>, Arc<JoinError>>;
#[derive(Clone, Debug)]
pub(crate) enum ResponseFailure {
    Body(Arc<EngineError>),
    Join(Arc<JoinError>),
}
#[derive(Debug)]
pub(crate) struct ResponseJoined {
    id: Id,
    failure: Option<ResponseFailure>,
}
impl ResponseJoined {
    pub(crate) fn id(&self) -> Id {
        self.id
    }
    pub(crate) fn failure(&self) -> Option<ResponseFailure> {
        self.failure.clone()
    }
}
#[derive(Default)]
struct Statistics {
    joined: usize,
    first: Option<ResponseFailure>,
}
#[derive(Default)]
pub(crate) struct ResponseTasks {
    records: Mutex<HashMap<Id, Weak<ResponseTask>>>,
    statistics: Mutex<Statistics>,
}
#[derive(Default)]
pub(crate) struct ResponseTask {
    state: Mutex<Registration>,
    changed: Notify,
    cancelled: std::sync::atomic::AtomicBool,
}
#[derive(Default)]
enum Registration {
    #[default]
    Idle,
    Pending,
    Active(AbortHandle),
    Joined(Arc<ResponseJoined>),
}
impl ResponseTask {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    /// A body-ready or aborted task cannot be rearmed until the original set
    /// has really consumed it. This prevents overlapping response receivers.
    fn reserve(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if self.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return false;
        }
        match &*state {
            Registration::Idle | Registration::Joined(_) => {
                *state = Registration::Pending;
                true
            }
            _ => false,
        }
    }
    pub(crate) fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        let task = match &*self.state.lock().unwrap_or_else(|error| error.into_inner()) {
            Registration::Active(task) => Some(task.clone()),
            _ => None,
        };
        if let Some(task) = task {
            task.abort();
        }
    }
    pub(crate) fn joined(&self) -> Option<Arc<ResponseJoined>> {
        match &*self.state.lock().unwrap_or_else(|error| error.into_inner()) {
            Registration::Joined(receipt) => Some(receipt.clone()),
            _ => None,
        }
    }
    pub(crate) async fn wait_joined(&self) -> Arc<ResponseJoined> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(joined) = self.joined() {
                return joined;
            }
            changed.await;
        }
    }
    fn consume(&self, id: Id, failure: Option<ResponseFailure>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if matches!(&*state, Registration::Active(handle) if handle.id() == id) {
            *state = Registration::Joined(Arc::new(ResponseJoined { id, failure }));
            drop(state);
            self.changed.notify_waiters();
        }
    }
}
impl ResponseTasks {
    pub(crate) fn spawn(
        &self,
        tasks: &mut JoinSet<ChainTask>,
        record: &Arc<ResponseTask>,
        future: impl Future<Output = ChainTask> + Send + 'static,
    ) {
        if !record.reserve() {
            return;
        }
        // Reserve before first poll. A panic in the actual registration leaves
        // Pending as Unknown; neither future Drop nor a marker completes it.
        let handle = tasks.spawn(future);
        let id = handle.id();
        *record
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Registration::Active(handle.clone());
        if record.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            handle.abort();
        }
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        records.retain(|_, record| record.strong_count() != 0);
        assert!(
            records.insert(id, Arc::downgrade(record)).is_none(),
            "one original response task identity"
        );
    }
    /// Called at the original relay's join_next_with_id consumption point,
    /// before forwarding its actual result. There is no second join consumer.
    pub(crate) fn observe(&self, result: NativeChainJoin) -> ObservedChainJoin {
        let id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let (result, failure) = match result {
            Ok((_, Ok(packet))) => (Ok(Ok(packet)), None),
            Ok((_, Err(source))) => {
                let source = Arc::new(source);
                (Ok(Err(source.clone())), Some(ResponseFailure::Body(source)))
            }
            Err(source) => {
                let source = Arc::new(source);
                (Err(source.clone()), Some(ResponseFailure::Join(source)))
            }
        };
        {
            let mut statistics = self
                .statistics
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            statistics.joined = statistics.joined.saturating_add(1);
            if statistics.first.is_none() {
                statistics.first = failure.clone();
            }
        }
        let record = self
            .records
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&id)
            .and_then(|record| record.upgrade());
        if let Some(record) = record {
            record.consume(id, failure);
        }
        result
    }
}
#[cfg(test)]
mod tests;
