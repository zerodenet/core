//! The original H1/H2 connection task polls its own finite XMUX/Hyper work.
//! No per-request worker, default TokioExecutor or second client pool.
mod driver;
mod executor;
use super::*;
use crate::split_http::{body::Body, client::carrier};
use driver::run;
use hyper_util::rt::TokioIo;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use tokio::sync::{oneshot, Notify};
use zero_platform_tokio::{
    contract::{
        scope::{TaskClass, TaskJoinWitness, TaskRegistrar},
        ChannelError, Impact, Reason, Stage,
    },
    controlled_carrier::{
        CarrierAdoption, CarrierDriver, CarrierOrigin, CarrierProtocolRetirement, CarrierUserView,
    },
    TcpRelayStream,
};
use zero_traits::packet_metadata::{
    PacketMetadataCredit, PacketMetadataError, PacketMetadataLayout, PacketMetadataWeak,
};
#[derive(Clone, Copy)]
pub enum TcpVersion {
    Http1,
    Http2 { keepalive: i64 },
}
type Driver = std::pin::Pin<Box<dyn std::future::Future<Output = hyper::Result<()>> + Send>>;
type Sender = oneshot::Sender<io::Result<carrier::Sender>>;
#[derive(Clone)]
pub struct PreparedXhttpTcp {
    state: PacketMetadataArc<State>,
    version: TcpVersion,
}
struct State {
    tickets: Mutex<Option<(PacketMetadataCredit, PacketMetadataCredit)>>,
    runtime: PacketMetadataArc<Runtime>,
    causes: PacketMetadataArc<sources::RequestCauses>,
    delegates: AtomicUsize,
    changed: Notify,
}
pub struct PendingXhttpTcp {
    ready: Option<oneshot::Receiver<io::Result<carrier::Sender>>>,
    state: PacketMetadataArc<State>,
    witness: TaskJoinWitness,
    retirement: CarrierProtocolRetirement,
    origin: CarrierOrigin,
    memory: Option<PacketMetadataCredit>,
}
pub struct OwnedXhttpTcp {
    sender: carrier::Sender,
    original: PacketMetadataArc<Original>,
}
pub(in crate::split_http) struct Original {
    state: PacketMetadataArc<State>,
    retirement: CarrierProtocolRetirement,
    origin: CarrierOrigin,
    witness: TaskJoinWitness,
}
impl PreparedXhttpTcp {
    pub fn new(
        book: CarrierBufferBudget,
        memory: PacketDriverMetadataView,
        version: TcpVersion,
    ) -> io::Result<Self> {
        // The actual XMUX group applies its existing concurrency grammar.
        // Zero/unlimited is bounded by this same Host metadata account, not
        // an unrelated QUIC endpoint-user setting or a new local pool. Every
        // concrete work Box and Node reserves that account before allocation;
        // each Node needs at least one allocation, so this passive global
        // upper bound cannot reject work earlier than the real Root limit.
        let limit = memory.snapshot().limit.allocations().max(1);
        let driver_layout = match version {
            TcpVersion::Http1 => PacketMetadataLayout::boxed::<
                hyper::client::conn::http1::Connection<TokioIo<TcpRelayStream>, Body>,
            >(),
            TcpVersion::Http2 { .. } => PacketMetadataLayout::boxed::<
                hyper::client::conn::http2::Connection<
                    TokioIo<TcpRelayStream>,
                    Body,
                    executor::Executor,
                >,
            >(),
        };
        let driver = memory.reserve(driver_layout).map_err(io::Error::other)?;
        let lease = memory
            .reserve(PacketMetadataArc::<Original>::layout().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        let header = memory
            .reserve(PacketMetadataArc::<State>::layout().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        let state = PacketMetadataArc::from_prepaid(
            State {
                tickets: Mutex::new(Some((driver, lease))),
                runtime: Runtime::prepare(book, memory.clone(), limit)?,
                causes: sources::RequestCauses::prepare(&memory)?,
                delegates: AtomicUsize::new(0),
                changed: Notify::new(),
            },
            header,
        )
        .map_err(|(source, _, _)| io::Error::other(source))?;
        Ok(Self { state, version })
    }
    pub fn start(
        &self,
        input: CarrierDriver<(TcpRelayStream, CarrierUserView)>,
        adoption: &CarrierAdoption,
        tasks: &TaskRegistrar,
        retirement: CarrierProtocolRetirement,
    ) -> Result<PendingXhttpTcp, ChannelError> {
        if !adoption.has_original_root() {
            return Err(failure(Stage::Prepare, Reason::StalePlan));
        }
        if self.state.runtime.stopped.is_cancelled() {
            return Err(failure(Stage::Prepare, Reason::Revoked));
        }
        let (driver, memory) = self
            .state
            .tickets
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| failure(Stage::Prepare, Reason::Revoked))?;
        // The ticket lock is gone before the actual registrar or any callback.
        let state = self.state.clone();
        let version = self.version;
        let (ready, receiver) = oneshot::channel();
        let (begin, started) = oneshot::channel();
        let (_, witness) = adoption.spawn_driver(
            tasks,
            TaskClass::Auxiliary,
            input,
            move |(io, view)| async move {
                // IO/future is actually destroyed before the original driver
                // user view is returned, on success, error and cancellation.
                let _view = view;
                if started.await.is_err() {
                    return Ok(());
                }
                run(io, state, version, ready, driver).await
            },
        )?;
        let origin = match adoption.origin(&witness) {
            Ok(origin) => origin,
            Err(source) => {
                // The same Slot already holds its actual registration. The
                // unadmitted body returns the original tuple, while Root
                // continues its real native/users/driver + Host ACK cleanup.
                drop(begin);
                retirement.request_retirement();
                return Err(source);
            }
        };
        // Real native task ID/witness is published by the original registrar
        // before handshake/executor delegation can poll the original input.
        let _ = begin.send(());
        Ok(PendingXhttpTcp {
            ready: Some(receiver),
            state: self.state.clone(),
            witness,
            retirement,
            origin,
            memory: Some(memory),
        })
    }
    pub fn stop(&self) {
        self.state.runtime.stop();
    }
}
impl PendingXhttpTcp {
    pub fn origin(&self) -> &CarrierOrigin {
        &self.origin
    }
    pub fn witness(&self) -> &TaskJoinWitness {
        &self.witness
    }
    pub async fn ready(&mut self) -> Result<OwnedXhttpTcp, ChannelError> {
        let sender = self
            .ready
            .as_mut()
            .ok_or_else(|| failure(Stage::Prepare, Reason::Revoked))?
            .await
            .map_err(|source| failure(Stage::Handshake, Reason::Revoked).with_source(source))?
            .map_err(|source| failure(Stage::Handshake, Reason::Io).with_source(source))?;
        self.ready.take();
        let credit = self
            .memory
            .take()
            .expect("single actual TCP sender handoff");
        let original = PacketMetadataArc::from_prepaid(
            Original {
                state: self.state.clone(),
                retirement: self.retirement.clone(),
                origin: self.origin.clone(),
                witness: self.witness.clone(),
            },
            credit,
        )
        .unwrap_or_else(|_| panic!("TCP lease header already paid"));
        Ok(OwnedXhttpTcp { sender, original })
    }
}
impl Drop for PendingXhttpTcp {
    fn drop(&mut self) {
        if self.memory.is_some() {
            self.state.runtime.stop();
            self.retirement.request_retirement();
        }
    }
}
impl Drop for Original {
    fn drop(&mut self) {
        self.state.runtime.stop();
        self.retirement.request_retirement();
    }
}
impl OwnedXhttpTcp {
    pub fn origin(&self) -> &CarrierOrigin {
        &self.original.origin
    }
    pub(in crate::split_http) fn into_parts(
        self,
    ) -> (carrier::Sender, PacketMetadataArc<Original>) {
        (self.sender, self.original)
    }
}
impl Original {
    pub(in crate::split_http) fn memory(&self) -> &PacketDriverMetadataView {
        &self.state.runtime.memory
    }
    pub(in crate::split_http) fn runtime(&self) -> PacketMetadataArc<Runtime> {
        self.state.runtime.clone()
    }
    pub(in crate::split_http) fn origin(&self) -> CarrierOrigin {
        self.origin.clone()
    }
    pub(in crate::split_http) fn retirement(&self) -> CarrierProtocolRetirement {
        self.retirement.clone()
    }
}
fn failure(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Component)
}
pub(super) struct DelegateReturn {
    state: PacketMetadataWeak<State>,
}
impl Drop for DelegateReturn {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            state.delegates.fetch_sub(1, Ordering::AcqRel);
            state.changed.notify_waiters();
        }
    }
}
