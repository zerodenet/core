//! Bounded warming in the original pool's actual TaskScope, never a caller's.
use super::{Opened, PreparedPreconnect};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::{
        component::PacketDriverMetadataView,
        context::OpenContext,
        network::{CarrierContext, PendingCarrierObserver, UpstreamBinding},
        scope::{TaskClass, TaskJoinWitness, TaskRegistrar},
        CancellationToken, ChannelError, Reason, Stage,
    },
    controlled_carrier::CarrierOrigin,
    TcpRelayStream,
};
use zero_traits::packet_metadata::PacketMetadataCredit;
use zero_transport::{
    pooled_driver::{ExclusiveIo, ManagedPool, Physical, PoolUse, PooledStream, UserStream},
    RuntimeError,
};
mod epoch;
mod worker;
struct Ready {
    worker: usize,
    binding: UpstreamBinding,
    io: TcpRelayStream,
    physical: Arc<Physical>,
    usage: PoolUse,
    expires: Instant,
    taken: tokio::sync::oneshot::Sender<bool>,
}
#[derive(Default)]
struct Queue {
    ready: VecDeque<Ready>,
    accepting: bool,
}
pub(crate) struct Native {
    pub(super) instance: ComponentInstanceId,
    pub(super) options: Arc<PreparedPreconnect>,
    pub(super) owner: Arc<ManagedPool>,
    pub(super) leaf: crate::transport::VlessOutboundLeaf,
    pub(super) tasks: TaskRegistrar,
    pub(super) memory: PacketDriverMetadataView,
    pub(super) stop: CancellationToken,
    queue: Mutex<Queue>,
    transition: tokio::sync::Mutex<()>,
    changed: Notify,
    workers: Mutex<(bool, Vec<TaskJoinWitness>)>,
    _credit: PacketMetadataCredit,
}
impl Native {
    pub(super) fn new(
        options: Arc<PreparedPreconnect>,
        owner: Arc<ManagedPool>,
        leaf: crate::transport::VlessOutboundLeaf,
        tasks: TaskRegistrar,
        memory: PacketDriverMetadataView,
        stop: CancellationToken,
    ) -> Result<Arc<Self>, ChannelError> {
        use zero_traits::packet_metadata::PacketMetadataLayout as Layout;
        let instance = owner.instance();
        let layout = Layout::arc::<Self>()
            .and_then(|l| l.checked_add(Layout::array::<Ready>(options.warm.get())?))
            .and_then(|l| l.checked_add(Layout::array::<TaskJoinWitness>(options.warm.get())?))
            .map_err(|source| error(instance, source))?;
        let credit = memory
            .reserve(layout)
            .map_err(|source| error(instance, source))?;
        let capacity = options.warm.get();
        Ok(Arc::new(Self {
            instance,
            options,
            owner,
            leaf,
            tasks,
            memory,
            stop,
            queue: Mutex::new(Queue {
                ready: VecDeque::with_capacity(capacity),
                accepting: false,
            }),
            transition: tokio::sync::Mutex::new(()),
            changed: Notify::new(),
            workers: Mutex::new((false, Vec::with_capacity(capacity))),
            _credit: credit,
        }))
    }
    pub(crate) fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub(crate) fn stop(&self) {
        self.stop.cancel();
        let retired = {
            let mut queue = self.queue.lock().unwrap();
            queue.accepting = false;
            std::mem::take(&mut queue.ready)
        };
        drop(retired);
        self.changed.notify_waiters();
    }
    fn start_workers(self: &Arc<Self>) -> Result<(), RuntimeError> {
        let mut workers = self.workers.lock().unwrap();
        if self.stop.is_cancelled() {
            return Err(std::io::Error::other("preconnect stopped").into());
        }
        if workers.0 {
            return Ok(());
        }
        // Persist start and each original witness synchronously. Interrupted
        // first demand cannot start a second set or discard partial spawning.
        workers.0 = true;
        for index in 0..self.options.warm.get() {
            let state = self.clone();
            match self.tasks.spawn_tracked(TaskClass::Auxiliary, async move {
                state.worker(index).await;
                Ok(())
            }) {
                Ok((_, witness)) => workers.1.push(witness),
                Err(source) => {
                    self.stop.cancel();
                    self.changed.notify_waiters();
                    return Err(std::io::Error::other(source).into());
                }
            }
        }
        Ok(())
    }
    async fn open_one(
        &self,
        binding: &UpstreamBinding,
        call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> Result<(TcpRelayStream, Arc<Physical>), RuntimeError> {
        let _operation = self.owner.operation()?;
        let leaf = self.leaf.clone();
        let instance = self.instance;
        let memory = self.memory.clone();
        let layered = self.owner.stream_dependency().is_some();
        self.owner
            .open_observed(
                &self.options.profile,
                binding,
                call,
                waiting,
                move |raw| async move {
                    let preparation = raw.preparation();
                    let io = TcpRelayStream::new(ExclusiveIo::new(raw));
                    let io = if layered {
                        io
                    } else {
                        leaf.open_preconnected_carrier(io, preparation, instance)
                            .await?
                    };
                    super::handoff::prepare(io, &memory)
                },
            )
            .await
    }
    pub(super) async fn take(
        self: &Arc<Self>,
        context: &OpenContext,
    ) -> Result<Opened, RuntimeError> {
        self.take_observed(
            context.upstream.binding(),
            CarrierContext {
                session: context.session_identity().map_err(std::io::Error::other)?,
                deadline: context.deadline,
                cancel: context.cancel.clone(),
            },
            context.upstream.observed_pending_carrier(),
        )
        .await
    }
    pub(super) async fn take_observed(
        self: &Arc<Self>,
        binding: &UpstreamBinding,
        call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> Result<Opened, RuntimeError> {
        self.epoch(binding).await?;
        if !binding.prefix.is_empty() {
            // Preserve relay semantics: it never borrows an unrelated warmed
            // root connection, but its real original belongs to this same pool.
            let (io, physical) = self
                .open_one(binding, call.clone(), waiting.clone())
                .await?;
            let queue = self.queue.lock().unwrap();
            self.owner.verify_epoch(binding)?;
            if !queue.accepting || self.stop.is_cancelled() {
                return Err(std::io::Error::other("preconnect epoch is retiring").into());
            }
            let usage = physical.user().map_err(std::io::Error::other)?;
            drop(queue);
            if let Some(waiting) = &waiting {
                physical
                    .observe_pending(waiting)
                    .map_err(std::io::Error::other)?;
            }
            self.owner.verify_epoch(binding)?;
            return opened(io, &physical, usage);
        }
        self.start_workers()?;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.epoch(binding).await?;
            loop {
                let ready = {
                    let mut queue = self.queue.lock().unwrap();
                    self.owner.verify_epoch(binding)?;
                    if !queue.accepting || self.stop.is_cancelled() {
                        return Err(std::io::Error::other("preconnect epoch is retiring").into());
                    }
                    queue.ready.pop_front()
                };
                let Some(ready) = ready else { break };
                if &ready.binding == binding && Instant::now() < ready.expires {
                    if let Some(waiting) = &waiting {
                        ready
                            .physical
                            .observe_pending(waiting)
                            .map_err(std::io::Error::other)?;
                    }
                    self.owner.verify_epoch(binding)?;
                    let io = opened(ready.io, &ready.physical, ready.usage)?;
                    let _ = ready.taken.send(true);
                    return Ok(io);
                }
                // Returning a queued view only wakes its original worker. That
                // worker still owes the actual physical close and Host ACK.
                drop(ready);
            }
            tokio::select! {biased;
                _=self.stop.cancelled()=>return Err(std::io::Error::other("preconnect stopped").into()),
                _=call.cancel.cancelled()=>return Err(std::io::Error::other("preconnect caller cancelled").into()),
                _=changed=>{},
            }
        }
    }
    pub(super) async fn drain(
        &self,
        end: Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError> {
        use zero_platform_tokio::contract::lifecycle::{CloseReport, Unfinished};
        self.stop();
        let mut report = CloseReport::default();
        let witnesses = self.workers.lock().unwrap().1.clone();
        for witness in witnesses {
            match tokio::time::timeout_at(end, witness.wait_registration_released()).await {
                Ok(joined) => {
                    if let Err(source) = joined.result() {
                        return Err(ChannelError::retained(source));
                    }
                }
                Err(_) => {
                    if let Some(id) = witness.task_id() {
                        report.unfinished.push(Unfinished::Task(id));
                    }
                }
            }
        }
        Ok(report)
    }
}
fn opened(
    io: TcpRelayStream,
    physical: &Arc<Physical>,
    usage: PoolUse,
) -> Result<Opened, RuntimeError> {
    let bypass = zero_traits::AsyncSocket::transport_bypass_control(&io);
    let settings = io.application_settings().cloned();
    let original: CarrierOrigin = physical.origin().map_err(std::io::Error::other)?;
    let (io, control, original) = PooledStream {
        io: Box::pin(UserStream::new(io, usage)),
        origin: original,
    }
    .into_driver_owned_parts()
    .map_err(std::io::Error::other)?;
    let io = ExclusiveIo::new(io);
    let io = match bypass {
        Some(control) => TcpRelayStream::with_transport_bypass_control(io, control),
        None => TcpRelayStream::new(io),
    };
    Ok(Opened {
        io: io.with_application_settings(settings),
        control,
        original,
    })
}
pub(super) fn error(
    instance: ComponentInstanceId,
    source: impl std::error::Error + Send + Sync + 'static,
) -> ChannelError {
    let mut error = ChannelError::new(
        Stage::Prepare,
        Reason::Io,
        zero_platform_tokio::contract::Impact::Component,
    );
    error.component = Some(instance.component);
    error.with_source(source)
}
