//! Compatibility control retains the same actual Source owner and its receipts.
use super::*;
use crate::runtime::inbound_owners::{
    system_tun::SYSTEM_TUN_OWNER, InboundOwner, InboundOwnerRegistry, InboundTerminal,
};
pub(super) struct SourceCompletion {
    registry: Arc<InboundOwnerRegistry>,
    original: Arc<InboundOwner>,
    collecting: Mutex<()>,
    terminal: std::sync::OnceLock<Arc<InboundTerminal>>,
    routes: Option<super::super::routes::owned::OwnedRoutes>,
    metadata: Arc<super::super::source::SystemTunSourceState>,
}
impl std::fmt::Debug for SourceCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemTunSourceCompletion")
            .field("original", &self.original.identity())
            .field("closed", &self.physically_closed())
            .finish_non_exhaustive()
    }
}
impl TunCompletion {
    pub(in crate::inbound::tun) fn matches_source(
        &self,
        original: zero_core::component::ComponentInstanceId,
    ) -> bool {
        self.source
            .as_ref()
            .is_some_and(|source| source.original.identity() == original)
    }
    #[cfg(test)]
    pub(in crate::inbound::tun) fn source_original(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::PacketProviderRegistration> {
        self.source.as_ref()?.metadata.original()
    }
    pub(in crate::inbound::tun) async fn prepare_source_network(
        &self,
        config: Arc<zero_config::RuntimeConfig>,
        deadline: Instant,
    ) -> Option<Result<super::super::PreparedTunNetwork, zero_engine::EngineError>> {
        let routes = self.source.as_ref()?.routes.as_ref()?;
        Some(
            routes
                .refresh_network(config, deadline)
                .await
                .map_err(|source| io::Error::other(source).into()),
        )
    }
    pub(in crate::inbound::tun) fn for_source(
        registry: Arc<InboundOwnerRegistry>,
        original: Arc<InboundOwner>,
        routes: Option<super::super::routes::owned::OwnedRoutes>,
        metadata: Arc<super::super::source::SystemTunSourceState>,
    ) -> Arc<Self> {
        let mut completion = Self::new_inner(None, None);
        Arc::get_mut(&mut completion)
            .expect("unpublished completion")
            .source = Some(SourceCompletion {
            registry,
            original,
            collecting: Mutex::new(()),
            terminal: std::sync::OnceLock::new(),
            routes,
            metadata,
        });
        completion
    }
}
impl SourceCompletion {
    pub(super) fn physically_closed(&self) -> bool {
        self.terminal.get().is_some_and(|terminal| {
            terminal.identity() == self.original.identity()
                && self.metadata.original().is_none_or(|original| {
                    original
                        .closed()
                        .is_some_and(|closed| closed.matches(&original))
                        && original.driver().is_joined()
                })
        })
    }
    pub(super) async fn wait_resources_closed(&self, deadline: Instant) -> io::Result<()> {
        let result = self.wait_closed(deadline).await;
        if self.physically_closed() {
            // The main/Scope path reports retained task faults once. These
            // compatibility waits observe the same physical resource outcome.
            Ok(())
        } else {
            result
        }
    }
    pub(super) async fn wait_closed(&self, deadline: Instant) -> io::Result<()> {
        timeout_at(deadline, async {
            let _collecting = self.collecting.lock().await;
            if self.terminal.get().is_none() {
                if let Some(owner) = self.registry.owner(SYSTEM_TUN_OWNER) {
                    if Arc::ptr_eq(&owner, &self.original) {
                        let receipt = self
                            .registry
                            .confirm_shutdown(SYSTEM_TUN_OWNER, deadline)
                            .await
                            .map_err(io::Error::other)?
                            .ok_or_else(|| {
                                io::Error::other(
                                    "same System TUN owner lost its exact shutdown receipt",
                                )
                            })?;
                        if receipt.terminal().identity() != self.original.identity() {
                            return Err(io::Error::other(
                                "System TUN close returned a different original owner",
                            ));
                        }
                        // Native/Scope completion and task faults are separate.
                        // Publish/release only this matching physical receipt;
                        // the same terminal retains every original task source.
                        self.registry
                            .release_closed(receipt, |_| Ok(()))
                            .map_err(io::Error::other)?;
                    }
                }
                let terminal = self.original.terminal().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "original System TUN Scope/users/native closure remains unconfirmed",
                    )
                })?;
                if terminal.identity() != self.original.identity() {
                    return Err(io::Error::other(
                        "System TUN terminal differs from its original owner",
                    ));
                }
                let _ = self.terminal.set(terminal);
            }
            match self
                .terminal
                .get()
                .expect("same physically closed terminal")
                .failure()
            {
                Some(source) => Err(io::Error::other(crate::component::instance::retained(
                    source,
                ))),
                None => Ok(()),
            }
        })
        .await
        .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
    }
}
