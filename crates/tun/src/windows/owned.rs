//! Persistent original adapter/session and the exact blocking reader handle.
mod acquire;
mod close;
mod io_view;
use crate::{OwnedTunIdentity, TunCloseReceipt};
pub use io_view::OwnedWintunIo;
use std::{
    io,
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

#[derive(Clone, Debug)]
pub enum WintunAcquisitionOutcome {
    KnownNoNative,
    Closed,
    Unknown,
}
struct State {
    attempted: bool,
    name: String,
    adapter: Option<Arc<wintun::Adapter>>,
    session: Option<Arc<wintun::Session>>,
    identity: Option<OwnedTunIdentity>,
    view_live: bool,
    reader: Option<JoinHandle<io::Result<()>>>,
    reader_joined: Option<Result<(), Arc<io::Error>>>,
    ended: Option<Arc<wintun::EndSessionReceipt>>,
    adapter_closed: Option<wintun::CloseAdapterReceipt>,
    receipt: Option<Arc<TunCloseReceipt>>,
    acquisition_failure: Option<Arc<io::Error>>,
    native_failure: Option<Arc<io::Error>>,
    errors: Vec<Arc<io::Error>>,
}
/// Install this finite owner before any open/create/start-session call. There
/// is one acquisition attempt, no driver fallback, retry pool or native escape.
pub struct OwnedWintunOwner {
    state: Mutex<State>,
    closing: std::sync::atomic::AtomicBool,
}
impl std::fmt::Debug for OwnedWintunOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedWintunOwner")
            .field("outcome", &self.outcome())
            .finish_non_exhaustive()
    }
}
impl OwnedWintunOwner {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                attempted: false,
                name: String::new(),
                adapter: None,
                session: None,
                identity: None,
                view_live: false,
                reader: None,
                reader_joined: None,
                ended: None,
                adapter_closed: None,
                receipt: None,
                acquisition_failure: None,
                native_failure: None,
                errors: Vec::new(),
            }),
            closing: std::sync::atomic::AtomicBool::new(false),
        })
    }
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn outcome(&self) -> WintunAcquisitionOutcome {
        let state = self.state();
        if state.native_failure.is_some()
            || state.view_live
            || state.reader.is_some()
            || state.session.is_some()
            || state.adapter.is_some()
        {
            WintunAcquisitionOutcome::Unknown
        } else if state.receipt.is_some() || state.adapter_closed.is_some() {
            WintunAcquisitionOutcome::Closed
        } else {
            WintunAcquisitionOutcome::KnownNoNative
        }
    }
    pub fn acquisition_failure(&self) -> Option<Arc<io::Error>> {
        self.state().acquisition_failure.clone()
    }
    pub fn closed_receipt(&self) -> Option<Arc<TunCloseReceipt>> {
        self.state().receipt.clone()
    }
}
fn shared(source: Arc<io::Error>) -> io::Error {
    io::Error::new(source.kind(), SharedSource(source))
}
#[cfg(test)]
mod tests;
#[derive(Debug)]
struct SharedSource(Arc<io::Error>);
impl std::fmt::Display for SharedSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
