//! One retained blocking Scope; the orchestration loop polls the sampler inline.
//! Cancelling a poll or shutdown waiter retains the original native join.
use std::{
    io,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use zero_platform_tokio::contract::{
    scope::{TaskClass, TaskJoinWitness, TaskScope},
    ChannelError,
};

#[derive(Default)]
pub(in crate::runtime) struct Sampler {
    state: tokio::sync::Mutex<Option<State>>,
    failure: Mutex<Option<Arc<ChannelError>>>,
    changed: tokio::sync::Notify,
    stopping: AtomicBool,
}
impl std::fmt::Debug for Sampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sampler").finish_non_exhaustive()
    }
}
struct State {
    engine: zero_engine::Engine,
    scope: TaskScope,
    tick: tokio::time::Interval,
    seconds: u64,
    endpoint_offset: Arc<AtomicUsize>,
    pending: Option<TaskJoinWitness>,
    closing: bool,
    closed: bool,
}
impl State {
    fn register(
        &mut self,
        work: impl FnOnce() -> Result<(), ChannelError> + Send + 'static,
    ) -> io::Result<()> {
        if self.closing || self.pending.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "the original statistics worker is retained",
            ));
        }
        let (_, witness) = self
            .scope
            .registrar()
            .spawn_blocking_tracked(TaskClass::Auxiliary, work)
            .map_err(io::Error::other)?;
        self.pending = Some(witness);
        Ok(())
    }
}
impl Sampler {
    pub(super) async fn start(&self, engine: zero_engine::Engine) -> io::Result<()> {
        let mut state = self.state.lock().await;
        if state.as_ref().is_some_and(|state| !state.closed) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "statistics sampling already has an original Scope",
            ));
        }
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        *state = Some(State {
            engine,
            scope: TaskScope::new(
                tokio::runtime::Handle::current(),
                NonZeroUsize::new(1).unwrap(),
            ),
            tick,
            seconds: 0,
            endpoint_offset: Arc::default(),
            pending: None,
            closing: false,
            closed: false,
        });
        *self.failure.lock().expect("statistics failure poisoned") = None;
        self.stopping.store(false, Ordering::Release);
        self.changed.notify_waiters();
        Ok(())
    }
    fn remember_failure(&self, witness: &TaskJoinWitness) {
        let Some(joined) = witness.try_joined() else {
            return;
        };
        let Err(error) = joined.result() else {
            return;
        };
        self.failure
            .lock()
            .expect("statistics failure poisoned")
            .get_or_insert(error);
        self.changed.notify_waiters();
    }
    fn result(&self) -> io::Result<()> {
        match self
            .failure
            .lock()
            .expect("statistics failure poisoned")
            .clone()
        {
            Some(error) => Err(io::Error::other(error)),
            None => Ok(()),
        }
    }
    /// Drive alongside the same pinned orchestration future, including its
    /// reload/network awaits. No additional owner task or runtime is created.
    pub(super) async fn drive<T>(
        &self,
        mut operation: zero_platform_tokio::contract::BoxFuture<'_, T>,
    ) -> T {
        let mut sampling = true;
        loop {
            tokio::select! {
                result = &mut operation => return result,
                sample = self.next_sample(), if sampling => {
                    if let Err(error) = sample {
                        // Actual worker failures already retain their original
                        // Arc. Preserve any registration error source as well.
                        self.failure.lock().expect("statistics failure poisoned").get_or_insert_with(|| {
                            Arc::new(ChannelError::new(
                                zero_platform_tokio::contract::Stage::Receive,
                                zero_platform_tokio::contract::Reason::Internal,
                                zero_platform_tokio::contract::Impact::Component,
                            ).with_source(error))
                        });
                        self.changed.notify_waiters();
                        sampling = false;
                    }
                }
            }
        }
    }
    pub(super) async fn observe_failure(&self) -> io::Error {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Err(error) = self.result() {
                return error;
            }
            changed.await;
        }
    }
    pub(super) async fn next_sample(&self) -> io::Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut state = self.state.lock().await;
            if self.stopping.load(Ordering::Acquire)
                || state.as_ref().is_none_or(|state| state.closing)
            {
                // Cold startup and completed generations grant no work. Release
                // the owner lock so start/shutdown can change this same owner.
                drop(state);
                changed.await;
                continue;
            }
            let state = state.as_mut().expect("started statistics Scope");
            if state.pending.is_none() {
                tokio::select! {
                    _ = &mut changed => continue,
                    _ = state.tick.tick() => {},
                }
                if self.stopping.load(Ordering::Acquire) {
                    continue;
                }
                let engine = state.engine.clone();
                let endpoint_due = state.seconds.is_multiple_of(10);
                state.seconds = state.seconds.wrapping_add(1);
                let offset = state.endpoint_offset.clone();
                state.register(move || {
                    #[cfg(feature = "host-network-stats")]
                    {
                        sample_host_interfaces(&engine);
                        engine.push_host_traffic_stats_sampled();
                    }
                    engine.push_stats_sampled();
                    engine.push_flow_updates();
                    if endpoint_due {
                        let next =
                            engine.push_endpoint_stats_sampled_page(offset.load(Ordering::Acquire));
                        offset.store(next, Ordering::Release);
                    }
                    Ok(())
                })?;
            }
            let exit = tokio::select! {
                _ = &mut changed => continue,
                exit = state.scope.next_exit() => exit,
            }
            .ok_or_else(|| {
                io::Error::other("statistics Scope ended before its retained worker joined")
            })?;
            let witness = state
                .pending
                .take()
                .expect("worker retained before its first await");
            debug_assert_eq!(witness.task_id(), Some(exit.id));
            self.remember_failure(&witness);
            return self.result();
        }
    }
    /// A running blocking operation cannot be aborted. Join this same original
    /// before resources it reads are closed. Cancelling the waiter retains its
    /// Scope, pending witness and any consumed failure for the next attempt.
    pub(super) async fn shutdown(&self) -> io::Result<()> {
        // Wake an interval or join poll before waiting for its owner lock. A
        // tick cannot admit another job after observing this stop fence.
        self.stopping.store(true, Ordering::Release);
        self.changed.notify_waiters();
        let mut state = self.state.lock().await;
        let Some(state) = state.as_mut() else {
            return Ok(());
        };
        state.closing = true;
        state.scope.begin_shutdown();
        while state.scope.next_exit().await.is_some() {
            if let Some(witness) = state.pending.as_ref() {
                self.remember_failure(witness);
            }
        }
        state.pending = None;
        state.closed = true;
        self.result()
    }
}

#[cfg(test)]
#[path = "tests/statistics.rs"]
mod tests;

#[cfg(feature = "host-network-stats")]
fn sample_host_interfaces(engine: &zero_engine::Engine) {
    let Ok(interfaces) = zero_platform_tokio::network_statistics::read() else {
        engine.observe_host_interfaces(None);
        return;
    };
    let sampled = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64;
    let samples = interfaces
        .into_iter()
        .map(|s| zero_engine::HostInterfaceSample {
            name: s.name,
            index: s.index,
            accounting_basis: s.accounting_basis,
            sampled_at_unix_ms: sampled,
            counters: [
                None,
                None,
                Some(s.rx_bytes),
                Some(s.tx_bytes),
                Some(s.rx_packets),
                Some(s.tx_packets),
                None,
                None,
                s.rx_dropped_packets,
                s.tx_dropped_packets,
                s.rx_errors,
                s.tx_errors,
            ],
        })
        .collect::<Vec<_>>();
    engine.observe_host_interfaces(Some(&samples));
}
