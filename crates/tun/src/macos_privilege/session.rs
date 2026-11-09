//! Same-source finite helper responsibility; it owns no Host or task authority.
use super::{
    peer_effective_uid, process, tool,
    transfer::{Original, Transfer, TransferOwner},
    HelperProgram, HelperRequest, HelperResponse, HelperSocketGuard,
};
use crate::packet_memory::{PacketMemoryView, PacketMetadataArc, PacketMetadataWeak};
use std::{
    io,
    os::fd::{FromRawFd, RawFd},
    os::unix::net::UnixStream,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Condvar, Mutex,
    },
};
mod access;
mod authorization;
pub(super) use access::current;
mod cleanup;
mod connection;
mod creation;
mod failures;
mod native;
mod rpc;
mod wake;
use failures::{Failures, Phase};

#[derive(Clone)]
pub struct MacosHelperOwner(PacketMetadataArc<Session>);
#[derive(Clone)]
pub struct MacosHelperAccess(PacketMetadataWeak<Session>);
pub struct MacosHelperLease {
    owner: MacosHelperOwner,
}
struct Session {
    epochs: [Mutex<Epoch>; 2],
    current: AtomicUsize,
    stopped: AtomicBool,
    route_held: AtomicBool,
    initial_driver_returned: AtomicBool,
    wake: Condvar,
    wake_io: Mutex<wake::Wake>,
    dedicated_worker: bool,
    wait: Mutex<()>,
    failures: PacketMetadataArc<Failures>,
    output_limit: usize,
}
struct Epoch {
    id: u64,
    native: native::Native,
    transfer: TransferOwner,
    tools: tool::Supervisor,
    rpc: rpc::Rpc,
    stage: Stage,
    command: u64,
    create_attempted: bool,
    shutdown_started: bool,
    local_tool: tool::ChildOriginal,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Cold,
    Accept,
    Hello,
    SupervisorRequest,
    SupervisorFd,
    SupervisorAck,
    Ready,
}
impl MacosHelperOwner {
    /// Concrete memory only, obtained from the same native provider's grant.
    /// All two-epoch fixed payloads/weak headers are paid before native IO.
    pub fn prepare(
        memory: &PacketMemoryView,
        output_limit: usize,
        dedicated_worker: bool,
    ) -> io::Result<Self> {
        if output_limit == 0 {
            return Err(io::Error::other("helper output allowance is zero"));
        }
        let owner_layout = PacketMetadataArc::<Session>::layout().map_err(io::Error::other)?;
        let failures_layout = PacketMetadataArc::<Failures>::layout().map_err(io::Error::other)?;
        let transfer_layout = PacketMetadataArc::<Transfer>::layout().map_err(io::Error::other)?;
        let all = owner_layout
            .checked_add(failures_layout)
            .and_then(|layout| layout.checked_add(transfer_layout))
            .and_then(|layout| layout.checked_add(transfer_layout))
            .map_err(io::Error::other)?;
        let mut credit = memory
            .reserve_driver_metadata(all)
            .map_err(io::Error::other)?;
        let faults = PacketMetadataArc::from_prepaid(
            Failures::new(),
            credit.split(failures_layout).map_err(io::Error::other)?,
        )
        .map_err(|(source, _, _)| io::Error::other(source))?;
        let mut epochs = [None, None];
        for slot in &mut epochs {
            let id = super::UTUN_HELPER_SEQUENCE
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                    value.checked_add(1)
                })
                .map_err(|_| io::Error::other("helper original sequence exhausted"))?;
            let transfer = TransferOwner::from_prepaid(
                id,
                credit.split(transfer_layout).map_err(io::Error::other)?,
            )?;
            *slot = Some(Mutex::new(Epoch {
                id,
                native: native::Native::new(),
                transfer,
                tools: tool::Supervisor::new(),
                rpc: rpc::Rpc::new(),
                stage: Stage::Cold,
                command: 0,
                create_attempted: false,
                shutdown_started: false,
                local_tool: tool::ChildOriginal::new(),
            }));
        }
        let [first, second] = epochs;
        let session = Session {
            epochs: [first.unwrap(), second.unwrap()],
            current: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
            route_held: AtomicBool::new(false),
            initial_driver_returned: AtomicBool::new(!dedicated_worker),
            wake: Condvar::new(),
            wake_io: Mutex::new(wake::Wake::new()),
            dedicated_worker,
            wait: Mutex::new(()),
            failures: faults,
            output_limit,
        };
        PacketMetadataArc::from_prepaid(session, credit)
            .map(Self)
            .map_err(|(source, _, _)| io::Error::other(source))
    }
    pub fn access(&self) -> MacosHelperAccess {
        MacosHelperAccess(PacketMetadataArc::downgrade(&self.0))
    }
    pub fn stop(&self) {
        self.0.stopped.store(true, Ordering::Release);
        self.0.notify();
    }
    /// Invoked only by the concrete runtime provider after matching its actual
    /// helper worker TaskJoined AND released registration/failure acceptance.
    /// Weak Access consumers cannot call this or install an unrelated witness.
    pub fn observe_initial_driver_returned(&self) {
        self.0
            .initial_driver_returned
            .store(true, Ordering::Release);
        self.0.notify();
    }
    pub fn lease_routes(&self) -> io::Result<MacosHelperLease> {
        if self.0.route_held.swap(true, Ordering::AcqRel) {
            return Err(io::Error::other(
                "same Source already has its route helper lease",
            ));
        }
        Ok(MacosHelperLease {
            owner: self.clone(),
        })
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        PacketMetadataArc::ptr_eq(&self.0, &other.0)
    }
    pub fn require_closed(&self) -> io::Result<()> {
        self.0.require_closed()
    }
    pub fn failure(&self) -> Option<zero_traits::packet_metadata::PacketMetadataError> {
        self.0
            .failures
            .any()
            .then(|| self.0.failures.clone().into_error())
    }
}
impl Drop for MacosHelperLease {
    fn drop(&mut self) {
        self.owner.0.route_held.store(false, Ordering::Release);
        self.owner.0.notify();
    }
}
impl Session {
    fn retain(&self, epoch: usize, phase: Phase, source: io::Error) {
        self.failures.retain(epoch, phase, source);
        self.notify();
    }
}
