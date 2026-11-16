//! Explicit API Sources use the same host allocator, native owner and observer.
use super::*;
use crate::{
    component::ComponentRecipe,
    inbound::system::source::{Factory, Options},
    protocol_registry::{PreparedInboundBind, TcpRuntimeServices},
    runtime::route_runtime::{InboundListenerRuntime, SharedIngressRuntimeServices},
};
use std::net::SocketAddr;
use zero_platform_tokio::contract::component::AcceptedTcpLimits;
fn source_key(tag: &str, serial: u64) -> String {
    format!("\0system-tcp:{serial}:{tag}")
}
#[cfg(all(test, feature = "socks5"))]
fn key(tag: &str) -> String {
    source_key(tag, 0)
}
impl InboundOwnerRegistry {
    pub(crate) async fn start_system_tcp(
        self: &Arc<Self>,
        protocols: &ProtocolInventory,
        config: &Arc<zero_config::RuntimeConfig>,
        services: TcpRuntimeServices,
        listen: SocketAddr,
        tag: &str,
    ) -> Result<(), EngineError> {
        // The API historically permits multiple listeners sharing a business
        // tag, including repeated port-zero calls. Each gets its own original
        // Source identity; the tag remains routing/observability input only.
        let serial = self
            .system_tcp_next
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
                |next| next.checked_add(1),
            )
            .map_err(|_| std::io::Error::other("System TCP source identity exhausted"))?;
        let key = source_key(tag, serial);
        let owner = self.install_system_tcp(protocols, config, services, &key, listen, tag)?;
        owner.wait_ready().await?;
        let actual = self
            .system_tcp_inputs
            .lock()
            .unwrap()
            .get(&key)
            .expect("installed API input")
            .1;
        tracing::info!(inbound_tag=tag,listen=%actual,"system inbound ready");
        Ok(())
    }
    fn install_system_tcp(
        self: &Arc<Self>,
        protocols: &ProtocolInventory,
        config: &Arc<zero_config::RuntimeConfig>,
        services: TcpRuntimeServices,
        key: &str,
        listen: SocketAddr,
        tag: &str,
    ) -> Result<Arc<InboundOwner>, EngineError> {
        let options = Options {
            listen,
            accepted: AcceptedTcpLimits {
                max_sockets: NonZeroUsize::new(
                    config.runtime.effective_inbound_max_accepted_sockets(),
                )
                .unwrap(),
                max_tasks_per_socket: NonZeroUsize::new(1).unwrap(),
                max_resources_per_socket: 0,
            },
        };
        let limit = options.accepted.max_sockets;
        let recipe = ComponentRecipe::from_validated(Arc::new(Factory), Arc::new(options));
        let prepared = PreparedInboundBind::from_recipe(recipe)?.with_literal_endpoint(listen)?;
        let (stop, _) = watch::channel(false);
        let mut lease = self.reserve_listener_kind(
            key,
            config,
            protocols,
            prepared,
            stop.clone(),
            NonZeroUsize::new(1).unwrap(),
            false,
        )?;
        let owner = lease.owner();
        // Everything after this reservation remains on the same original even
        // if binding/build/start fails or the public readiness waiter is dropped.
        let installed = (|| {
            lease.bind_native()?;
            let leased = lease.finish()?;
            let (recipe, view, installed) = leased.promote_definition()?;
            let actual = match &view {
                NativeListenerView::Tcp(listener) => listener.local_addr()?,
                _ => {
                    return Err(
                        std::io::Error::other("System TCP requires its original TCP view").into(),
                    )
                }
            };
            let runtime = InboundListenerRuntime::new(
                SharedIngressRuntimeServices::new(services),
                tag.into(),
                Some(owner.task_access()),
            )
            .with_inbound_instance(owner.identity())
            .with_readiness(owner.readiness());
            let (source, peer) = installed.build_inbound(
                recipe,
                view,
                Some(
                    runtime
                        .route_factory()
                        .for_connection(None)
                        .upstream_services(),
                ),
            )?;
            let runtime = runtime.with_datagram_source(peer);
            owner.spawn_driver(async move {
                crate::runtime::inbound_operation::source::run(
                    source,
                    runtime,
                    stop.subscribe(),
                    limit,
                    "system",
                )
                .await
            })?;
            owner.seal_listener_installation();
            self.observers.enqueue_system_tun(owner.clone(), None)?;
            self.system_tcp_inputs
                .lock()
                .unwrap()
                .insert(key.into(), (tag.into(), actual));
            Ok(())
        })();
        if let Err(source) = installed {
            return Err(owner.retain_managed_listener_error(source));
        }
        Ok(owner)
    }
    pub(in crate::runtime) fn system_tcp_owners(&self) -> Vec<(String, Arc<InboundOwner>)> {
        let tags = self
            .system_tcp_inputs
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        tags.into_iter()
            .filter_map(|tag| self.owner(&tag).map(|owner| (tag, owner)))
            .collect()
    }
    pub(in crate::runtime) fn system_tcp_budget_changed(
        &self,
        old: &zero_config::RuntimeConfig,
        next: &zero_config::RuntimeConfig,
    ) -> bool {
        !self.system_tcp_inputs.lock().unwrap().is_empty()
            && (crate::runtime::orchestration::carrier_budget_changed(old, next)
                || old.runtime.effective_inbound_max_accepted_sockets()
                    != next.runtime.effective_inbound_max_accepted_sockets())
    }
    pub(in crate::runtime) async fn retire_system_tcp_reported(
        &self,
        deadline: Instant,
        expected: &mut usize,
        runtime: &crate::runtime::route_runtime::InboundListenerRuntimeFactory,
    ) -> Result<(), EngineError> {
        for (tag, _) in self.system_tcp_owners() {
            if let Some(receipt) = self.confirm_shutdown(&tag, deadline).await? {
                if receipt.terminal().joined_tasks() > 0 && receipt.terminal().failure().is_none() {
                    *expected = expected.saturating_add(1);
                }
                self.release_closed_reported(receipt, |terminal| {
                    runtime.publish_inbound_terminal(&tag, terminal);
                    Ok(())
                })?;
            }
        }
        Ok(())
    }
    /// Cold API input survives a run restart, without retaining an old config,
    /// recipe, private runtime or native capability. Old owners close first.
    pub(crate) async fn restore_system_tcp(
        self: &Arc<Self>,
        protocols: &ProtocolInventory,
        config: &Arc<zero_config::RuntimeConfig>,
        services: TcpRuntimeServices,
    ) -> Result<(), EngineError> {
        let inputs = self
            .system_tcp_inputs
            .lock()
            .unwrap()
            .iter()
            .map(|(key, (tag, listen))| (key.clone(), tag.clone(), *listen))
            .collect::<Vec<_>>();
        for (key, tag, listen) in inputs {
            let owner = match self.owner(&key) {
                Some(owner) => owner,
                None => self.install_system_tcp(
                    protocols,
                    config,
                    services.clone(),
                    &key,
                    listen,
                    &tag,
                )?,
            };
            owner.wait_ready().await?;
        }
        Ok(())
    }
}

impl crate::runtime::Proxy {
    pub(crate) async fn start_system_tcp_source(
        &self,
        listen: SocketAddr,
        tag: &str,
    ) -> Result<(), EngineError> {
        let _apply = self.host.apply_lock().lock().await;
        let config = self.engine().config();
        self.inbound_owners
            .start_system_tcp(
                &self.protocols,
                &config,
                self.tcp_runtime_services(),
                listen,
                tag,
            )
            .await
    }
}
#[cfg(all(test, feature = "socks5"))]
#[path = "system_tcp/tests.rs"]
mod tests;
