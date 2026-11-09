//! One original response task per association, not one detached task per path.
use crate::protocol_registry::TcpExecutionServices;
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{future::Future, pin::Pin, sync::Mutex};
use zero_engine::EngineError;
use zero_platform_tokio::contract::component::{PacketDriverMetadataView, PacketMetadataVec};
use zero_platform_tokio::contract::scope::TaskJoinWitness;
use zero_platform_tokio::contract::RetainedError;
use zero_platform_tokio::contract::{CancellationToken, ChannelError, Impact, Reason, Stage};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit};

mod driver;
mod faults;
mod preparation;
use driver::run;
pub(super) use faults::PathStatus;
use faults::{Failures, OriginalFailures, PathFailure};
type Receive = Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send>>;
struct PaidReceive {
    work: Receive,
    _credit: PacketMetadataCredit,
}
impl Future for PaidReceive {
    type Output = Result<(), ChannelError>;
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().work.as_mut().poll(cx)
    }
}
struct Shared {
    pending: Mutex<PacketMetadataVec<PaidReceive>>,
    changed: tokio::sync::Notify,
    stop: CancellationToken,
    failures: PacketMetadataArc<Failures>,
}
pub(super) struct Receiver {
    original: TaskJoinWitness,
    witness: TaskJoinWitness,
    shared: PacketMetadataArc<Shared>,
    memory: PacketDriverMetadataView,
    max_paths: usize,
    buffers: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
}
pub(super) struct PreparedReceive {
    source: PacketMetadataArc<PathFailure>,
    failures: PacketMetadataArc<Failures>,
    memory: PacketDriverMetadataView,
    submitted: bool,
}
pub(super) struct ReceiveScratch {
    pub(super) bytes: Vec<u8>,
    _credit: zero_platform_tokio::controlled_carrier::CarrierBufferCredit,
}
impl Receiver {
    pub(super) fn start(services: &TcpExecutionServices) -> Result<Self, EngineError> {
        let invocation = services
            .invocation
            .as_ref()
            .ok_or_else(|| failure(rejected(Reason::InvalidConfig)))?;
        let caller = caller(services)?;
        let host = services
            .current_source
            .upgrade()
            .ok_or_else(|| failure(rejected(Reason::Revoked)))?;
        let (buffers, memory) = host.packet_path_memory().map_err(EngineError::from)?;
        let layout = PacketMetadataArc::<Failures>::layout().map_err(memory_failure)?;
        let credit = memory.reserve(layout).map_err(memory_failure)?;
        let failures = PacketMetadataArc::from_prepaid(Failures::default(), credit)
            .unwrap_or_else(|_| panic!("same exact prepaid response-source layout"));
        let shared_credit = memory
            .reserve(PacketMetadataArc::<Shared>::layout().map_err(memory_failure)?)
            .map_err(memory_failure)?;
        let shared = PacketMetadataArc::from_prepaid(
            Shared {
                pending: Mutex::new(PacketMetadataVec::default()),
                changed: Default::default(),
                // Cleanup only. The owning manager is the actual association,
                // not the first Business session. Original grant/Scope fences keep
                // governing IO; manager Drop requests this one actor to return RX.
                stop: CancellationToken::new(),
                failures,
            },
            shared_credit,
        )
        .unwrap_or_else(|_| panic!("exact original response actor prepaid"));
        let actual = shared.clone();
        #[cfg(feature = "raw-ip-runtime")]
        let registered = if let Some(native) = &invocation.caller_native {
            native.spawn_datagram_receiver(move |_| run(actual))
        } else if let Some(accepted) = &invocation.caller_accepted {
            accepted.spawn(
                zero_platform_tokio::contract::scope::TaskClass::Auxiliary,
                move |_| run(actual),
            )
        } else {
            Err(rejected(Reason::Unsupported))
        };
        #[cfg(not(feature = "raw-ip-runtime"))]
        let registered = if let Some(accepted) = &invocation.caller_accepted {
            accepted.spawn(
                zero_platform_tokio::contract::scope::TaskClass::Auxiliary,
                move |_| run(actual),
            )
        } else {
            Err(rejected(Reason::Unsupported))
        };
        let (_, receiver) = registered.map_err(failure)?;
        if !caller
            .scope_identity()
            .same_scope(&receiver.scope_identity())
        {
            shared.stop.cancel();
            return Err(failure(rejected(Reason::StalePlan)));
        }
        Ok(Self {
            original: caller,
            witness: receiver,
            shared,
            memory,
            max_paths: services.retired_datagrams().capacity(),
            buffers,
        })
    }
    pub(super) fn check_caller(&self, services: &TcpExecutionServices) -> Result<(), EngineError> {
        if !self.original.same_registration(&caller(services)?)
            || self.witness.is_joined()
            || self.shared.stop.is_cancelled()
        {
            return Err(failure(rejected(Reason::StalePlan)));
        }
        Ok(())
    }
    pub(super) fn prepare_path(&self) -> Result<(PreparedReceive, ReceiveScratch), EngineError> {
        if self.witness.is_joined() || self.shared.stop.is_cancelled() {
            return Err(failure(rejected(Reason::Revoked)));
        }
        let scratch_credit = self
            .buffers
            .try_reserve(64 * 1024)
            .map_err(EngineError::from)?;
        let layout = PacketMetadataArc::<PathFailure>::layout().map_err(memory_failure)?;
        let error_layout = RetainedError::metadata_layout().map_err(memory_failure)?;
        let total = layout.checked_add(error_layout).map_err(memory_failure)?;
        let mut credit = self.memory.reserve(total).map_err(memory_failure)?;
        let error_credit = credit.split(error_layout).map_err(memory_failure)?;
        let source = PacketMetadataArc::from_prepaid(
            PathFailure {
                original: std::sync::OnceLock::new(),
                credit: Mutex::new(Some(error_credit)),
            },
            credit,
        )
        .unwrap_or_else(|_| panic!("same exact paid response-error cell"));
        let _notifications = self.memory.notifications();
        let mut slots = self
            .shared
            .failures
            .paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        slots
            .try_reserve_one_with_metadata(&self.memory, self.max_paths)
            .map_err(memory_failure)?;
        slots
            .push_reserved(source.clone())
            .unwrap_or_else(|_| panic!("prepaid original source slot"));
        drop(slots);
        let scratch = ReceiveScratch {
            bytes: vec![0; 64 * 1024],
            _credit: scratch_credit,
        };
        Ok((
            PreparedReceive {
                source,
                failures: self.shared.failures.clone(),
                memory: self.memory.clone(),
                submitted: false,
            },
            scratch,
        ))
    }
    pub(super) fn retain(
        &self,
        mut prepared: PreparedReceive,
        future: impl Future<Output = Result<(), ChannelError>> + Send + 'static,
    ) -> Result<PathStatus, EngineError> {
        if self.witness.is_joined() || self.shared.stop.is_cancelled() {
            return Err(failure(rejected(Reason::Revoked)));
        }
        let status = PathStatus {
            source: prepared.source.clone(),
            failures: self.shared.failures.clone(),
            memory: self.memory.clone(),
        };
        let source = prepared.source.clone();
        let failures = self.shared.failures.clone();
        let work = async move {
            if let Err(actual) = future.await {
                let credit = source
                    .credit
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .expect("one actual receive result per path");
                let actual = RetainedError::from_prepaid(actual, credit).unwrap_or_else(|_| {
                    panic!("exact response-error allocation prepaid before IO")
                });
                let _ = source.original.set(actual.clone());
                let _ = failures.first.set(actual);
            }
            Ok(())
        };
        let bytes = std::mem::size_of_val(&work);
        let credit = self
            .memory
            .reserve(zero_traits::packet_metadata::PacketMetadataLayout::new(
                usize::from(bytes != 0),
                bytes,
            ))
            .map_err(memory_failure)?;
        let work = PaidReceive {
            work: Box::pin(work),
            _credit: credit,
        };
        let _notifications = self.memory.notifications();
        let mut pending = self
            .shared
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pending
            .try_reserve_one_with_metadata(&self.memory, self.max_paths)
            .map_err(memory_failure)?;
        pending
            .push_reserved(work)
            .unwrap_or_else(|_| panic!("prepaid original response work slot"));
        drop(pending);
        prepared.submitted = true;
        self.shared.changed.notify_one();
        Ok(status)
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.shared.stop.cancel();
    }
}
fn caller(services: &TcpExecutionServices) -> Result<TaskJoinWitness, EngineError> {
    let call = services
        .invocation
        .as_ref()
        .ok_or_else(|| failure(rejected(Reason::InvalidConfig)))?;
    #[cfg(feature = "raw-ip-runtime")]
    if let Some(native) = &call.caller_native {
        native.check_open().map_err(failure)?;
        return native
            .caller_witness()
            .ok_or_else(|| failure(rejected(Reason::Revoked)));
    }
    if let Some(accepted) = &call.caller_accepted {
        accepted.check_open().map_err(failure)?;
        return accepted
            .task_join_witness()
            .ok_or_else(|| failure(rejected(Reason::Revoked)));
    }
    Err(failure(rejected(Reason::Unsupported)))
}
fn rejected(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Session)
}
fn failure(source: ChannelError) -> EngineError {
    std::io::Error::other(source).into()
}

fn memory_failure(source: zero_traits::packet_metadata::PacketMemoryError) -> EngineError {
    failure(rejected(Reason::Capacity).with_source(source))
}
