//! A live carrier binding is checked without scheduling another connection.
use crate::DnsCarrierBinding;
use std::{
    io,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use zero_platform_tokio::contract::CancellationToken;

pub(in super::super) struct ClientState {
    usable: AtomicBool,
    pub(in super::super) reusable: bool,
    pub(in super::super) leases: AtomicUsize,
    pub(in super::super) cancel: CancellationToken,
    pub(super) changed: Arc<tokio::sync::Notify>,
    binding: Option<Arc<dyn DnsCarrierBinding>>,
    egress: zero_platform_tokio::EgressInterfaceControl,
    generation: u64,
}
impl ClientState {
    pub(in super::super) fn new(
        changed: Arc<tokio::sync::Notify>,
        binding: Option<Arc<dyn DnsCarrierBinding>>,
        direct: bool,
        egress: zero_platform_tokio::EgressInterfaceControl,
        generation: u64,
    ) -> Self {
        Self {
            usable: AtomicBool::new(true),
            reusable: direct || binding.is_some(),
            leases: AtomicUsize::new(0),
            cancel: CancellationToken::new(),
            changed,
            binding,
            egress,
            generation,
        }
    }
    pub(in super::super) fn retire(&self) {
        self.usable.store(false, Ordering::Release);
        self.cancel.cancel();
        self.changed.notify_waiters();
    }
    pub(super) fn ensure_current(&self) -> io::Result<()> {
        if self.egress.generation() != self.generation {
            self.retire();
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "actual DoH physical egress generation changed",
            ));
        }
        if !self.usable.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "DoH connection was retired",
            ));
        }
        if let Some(binding) = &self.binding {
            if let Err(source) = binding.check_current() {
                self.retire();
                return Err(source);
            }
        }
        Ok(())
    }
    pub(super) fn is_reusable(&self) -> bool {
        self.reusable && self.ensure_current().is_ok()
    }
    pub(in super::super) async fn revoked(&self) -> io::Error {
        match self.binding.as_deref() {
            Some(binding) => binding.revoked().await,
            None => {
                let mut changes = self.egress.subscribe_generation();
                loop {
                    if self.egress.generation() != self.generation {
                        return io::Error::new(
                            io::ErrorKind::Interrupted,
                            "actual DoH physical egress generation changed",
                        );
                    }
                    if let Err(source) = changes.changed().await {
                        return io::Error::new(io::ErrorKind::NotConnected, source);
                    }
                }
            }
        }
    }
}
pub(super) async fn revoked(binding: Option<&dyn DnsCarrierBinding>) -> io::Error {
    match binding {
        Some(binding) => binding.revoked().await,
        None => std::future::pending().await,
    }
}
