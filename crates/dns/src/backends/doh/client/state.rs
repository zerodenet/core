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
    pub(in super::super) native: std::sync::OnceLock<super::super::native::NativeClient>,
    usable: AtomicBool,
    #[cfg(feature = "test-support")]
    pub(in super::super) panic_original_driver: CancellationToken,
    pub(in super::super) reusable: bool,
    pub(in super::super) leases: AtomicUsize,
    pub(in super::super) cancel: CancellationToken,
    pub(super) changed: Arc<tokio::sync::Notify>,
    binding: std::sync::OnceLock<Arc<dyn DnsCarrierBinding>>,
    egress: zero_platform_tokio::EgressInterfaceControl,
    generation: u64,
}
impl ClientState {
    pub(in super::super) fn retained_instances(
        &self,
    ) -> io::Result<Vec<zero_core::component::ComponentInstanceId>> {
        let native = self
            .native
            .get()
            .ok_or_else(|| io::Error::other("actual DoH native client unavailable"))?;
        if native.retired() {
            return Ok(Vec::new());
        }
        let mut instances = native
            .retained_instances()?
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        if let Some(binding) = self.binding.get() {
            instances.extend(binding.component_instances().ok_or_else(|| {
                io::Error::other("actual detour cannot observe its retained role instances")
            })?);
        }
        Ok(instances.into_iter().collect())
    }
    pub(in super::super) fn new(
        changed: Arc<tokio::sync::Notify>,
        binding: Option<Arc<dyn DnsCarrierBinding>>,
        direct: bool,
        egress: zero_platform_tokio::EgressInterfaceControl,
        generation: u64,
    ) -> Self {
        Self {
            native: Default::default(),
            usable: AtomicBool::new(true),
            #[cfg(feature = "test-support")]
            panic_original_driver: CancellationToken::new(),
            reusable: direct || binding.is_some(),
            leases: AtomicUsize::new(0),
            cancel: CancellationToken::new(),
            changed,
            binding: binding.map(std::sync::OnceLock::from).unwrap_or_default(),
            egress,
            generation,
        }
    }
    pub(in super::super) fn retire(&self) {
        if self.usable.swap(false, Ordering::AcqRel) {
            self.cancel.cancel();
            self.changed.notify_waiters();
        }
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
        if self
            .native
            .get()
            .is_some_and(|native| !native.retains_live_original())
        {
            self.retire();
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "DoH actual driver/native custody is no longer retained",
            ));
        }
        if let Some(binding) = self.binding.get() {
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
        match self.binding.get().map(Arc::as_ref) {
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
impl ClientState {
    pub(in super::super) fn bind(&self, binding: Arc<dyn DnsCarrierBinding>) -> io::Result<()> {
        self.binding
            .set(binding)
            .map_err(|_| io::Error::other("DoH physical binding installed twice"))
    }
}
pub(super) async fn revoked(binding: Option<&dyn DnsCarrierBinding>) -> io::Error {
    match binding {
        Some(binding) => binding.revoked().await,
        None => std::future::pending().await,
    }
}
