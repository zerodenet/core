//! Ordinary TUN command/configuration assembly uses the same retained Source.
use super::*;
use std::num::NonZeroUsize;
use tokio::time::{timeout_at, Instant};
use zero_platform_tokio::contract::component::PacketDeviceBinding;
#[derive(Clone, Debug)]
pub(super) struct Start {
    pub name: Option<String>,
    pub raw_addr: String,
    pub addresses: Vec<config::TunInterfaceAddress>,
    pub route_addresses: Vec<(IpAddr, IpAddr)>,
    pub mtu: u16,
    pub tag: String,
    pub options: TunRuntimeOptions,
    pub managed_config: Option<zero_config::TunConfig>,
    pub dns_hijacked_queries: Arc<AtomicU64>,
}
impl Proxy {
    pub(super) async fn prepare_live_tun_network(
        &self,
        desired: &zero_config::TunConfig,
    ) -> Result<PreparedTunNetwork, EngineError> {
        if !desired.auto_route {
            let (dns_hijack, route_exclusions) = config::prepare_dns_routes(
                self.engine().config().as_ref(),
                desired.dns_hijack,
                false,
                || unreachable!("automatic routes disabled: no system DNS discovery"),
            )?;
            return Ok(PreparedTunNetwork {
                dns_hijack,
                route_exclusions,
            });
        }
        let completion = self
            .tun_control
            .lock()
            .unwrap()
            .as_ref()
            .map(|control| control.completion.clone())
            .ok_or_else(|| io::Error::other("live TUN has no original Source completion"))?;
        completion
            .prepare_source_network(self.engine().config(), deadline())
            .await
            .ok_or_else(|| io::Error::other("live TUN has no original route worker"))?
    }
    pub(super) async fn start_managed_tun(
        &self,
        start: Start,
        config: Arc<zero_config::RuntimeConfig>,
        recovery: Option<Arc<budget::TunBudgetRecovery>>,
    ) -> Result<(), EngineError> {
        let id = NEXT_TUN_ID
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| io::Error::other("System TUN identity space is exhausted"))?;
        let address_pairs = start
            .addresses
            .iter()
            .map(|address| (address.address, address.netmask))
            .collect::<Vec<_>>();
        let actual_tasks =
            crate::runtime::inbound_operation::raw_ip::PacketSourceContext::device_task_limit()
                .checked_add(1 + if start.options.auto_route { 2 } else { 0 })
                .ok_or_else(|| io::Error::other("System TUN actual task ceiling overflow"))?;
        let limit = NonZeroUsize::new(config.runtime.effective_system_tun_task_limit(actual_tasks))
            .ok_or_else(|| io::Error::other("System TUN task ceiling must be positive"))?;
        let native_io = statistics::TunTraffic::prepare(self.engine(), &start.tag);
        let source = source::SystemTunSourceOptions::new(
            "system.tun.original".into(),
            PacketDeviceBinding::Create {
                name: start.name.clone().map(Into::into),
                addresses: address_pairs.clone(),
                mtu: start.mtu,
            },
            address_pairs,
            start.mtu,
            start.tag.clone(),
            limit,
            native_io.clone(),
        )
        .map_err(io::Error::other)?;
        let metadata = source.state();
        let recipe = crate::component::ComponentRecipe::from_validated(
            Arc::new(source::SystemTunSourceFactory),
            source.clone(),
        );
        let registry = self.system_tun_source_registry();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let lease = registry.reserve_system_tun(&self.protocols, recipe, shutdown.clone())?;
        let original = lease.owner();
        let routes = start.options.auto_route.then(|| {
            routes::owned::OwnedRoutes::new(
                self.clone(),
                routes::RouteRuntimeSpec {
                    id,
                    tun_name: String::new(),
                    recovery_key: start.tag.clone(),
                    primary_ipv6: start.addresses[0].address.is_ipv6(),
                    addresses: start.route_addresses.clone(),
                    include_cidrs: start.options.include_cidrs.clone(),
                    exclude_cidrs: start.options.exclude_cidrs.clone(),
                    dns_hijack: start.options.dns_hijack,
                    strict_route: start.options.strict_route,
                },
                metadata.clone(),
                original.task_access().cancellation().child_token(),
            )
        });
        if let Some(routes) = &routes {
            lease.register_after_scope(routes.resource())?;
        }
        let completion = completion::TunCompletion::for_source(
            registry.clone(),
            original.clone(),
            routes.clone(),
            metadata.clone(),
        );
        completion.install_restart(Arc::new(start.clone()));
        // Same exact native/Scope owner and cleanup path are durable before
        // discovery, driver registration, device acquisition or any await.
        *self.tun_control.lock().unwrap() = Some(TunControl {
            id,
            shutdown: shutdown.clone(),
            completion: completion.clone(),
            budget_recovery: recovery,
            route_recovery: None,
        });
        let result=async {
            let (route_recovery,recovery)=tokio::sync::mpsc::channel(1);
            let prepared=if let Some(routes)=&routes{
                let tasks=lease.preparation_tasks()?;
                routes.prepare(&tasks,config.clone(),recovery).map_err(io::Error::other)?;
                routes.prepared(deadline()).await.map_err(io::Error::other)?
            }else{
                let prepared=config::prepare_dns_routes(config.as_ref(),start.options.dns_hijack,false,||{
                    unreachable!("automatic routes disabled: no system DNS discovery is needed")
                });
                match prepared{
                    Ok((dns_hijack,route_exclusions))=>PreparedTunNetwork{dns_hijack,route_exclusions},
                    Err(source)if start.options.strict_route=>return Err(EngineError::Io(source)),
                    Err(source)=>{tracing::warn!(%source,"TUN DNS interception disabled after preparation failure");PreparedTunNetwork{dns_hijack:false,route_exclusions:vec![]}},
                }
            };
            let dns_hijacked_queries=start.dns_hijacked_queries.clone();
            let ingress=source.ingress_options(prepared.dns_hijack,dns_hijacked_queries.clone());
            let runtime=crate::runtime::route_runtime::InboundListenerRuntime::new(
                crate::runtime::route_runtime::SharedIngressRuntimeServices::new(self.tcp_runtime_services().for_inbound(start.tag.clone())),
                start.tag.clone(),None,
            );
            let owner=lease.start(runtime,shutdown_rx,ingress,Some(self.system_tun_exit_projection(start.managed_config.is_some())))?;
            if !Arc::ptr_eq(&owner,&original){return Err(io::Error::other("System TUN start changed its actual owner").into());}
            timeout_at(deadline(),owner.wait_ready()).await.map_err(|source|io::Error::new(io::ErrorKind::TimedOut,source))??;
            let route_state=match &routes{
                Some(routes)=>Some(routes.ready(deadline()).await.map_err(io::Error::other)?),None=>None,
            };
            let name=metadata.interface().ok_or_else(||io::Error::other("ready System TUN omitted its original interface"))?;
            let provider=metadata.original().ok_or_else(||io::Error::other("ready System TUN omitted its actual native original"))?;
            if provider.instance()!=owner.identity()||metadata.driver().is_none_or(|driver|!driver.same_registration(provider.driver())){
                return Err(io::Error::other("System TUN ready facts differ from the original native owner/driver").into());
            }
            let before=self.egress_interface.generation();
            self.egress_interface.replace_tunnel_addresses(start.addresses.iter().map(|address|address.address));
            log_tun_generation_change(&self.egress_interface,before,"tun_addresses_published",None);
            let v4=route_state.as_ref().and_then(|state|state.v4.clone());
            let v6=route_state.as_ref().and_then(|state|state.v6.clone());
            let egress=if start.addresses[0].address.is_ipv6(){v6.clone().or_else(||v4.clone())}else{v4.clone().or_else(||v6.clone())};
            // Serialize the compatibility publication with the exact main-exit
            // projection. A Ready notification is not permission to republish
            // a Source that already stopped before its command ACK.
            owner.wait_ready().await?;
            let mut controls=self.tun_control.lock().unwrap();
            let control=controls.as_mut().filter(|control|control.id==id && Arc::ptr_eq(&control.completion,&completion))
                .ok_or_else(||io::Error::other("System TUN original completion changed before publication"))?;
            if owner.task_access().cancellation().is_cancelled() {
                return Err(owner.ready_stopped());
            }
            *self.tun_info.lock().unwrap()=Some(TunInfo{
                id,name:name.to_string(),addr:start.raw_addr,addresses:start.addresses.iter().map(|address|address.cidr.clone()).collect(),
                mtu:start.mtu,tag:start.tag.clone(),auto_route:start.options.auto_route,include_cidrs:start.options.include_cidrs,
                exclude_cidrs:start.options.exclude_cidrs,dual_stack:start.options.dual_stack,strict_route:start.options.strict_route,
                dns_hijack:prepared.dns_hijack,dns_hijacked_queries,
                healthy:route_state.as_ref().is_none_or(|state|state.healthy),last_error:route_state.as_ref().and_then(|state|state.error.clone()),
                egress_interface:egress,egress_interface_v4:v4,egress_interface_v6:v6,
                route_exclusions:route_state.map_or(prepared.route_exclusions,|state|state.exclusions),managed_config:start.managed_config,
            });
            control.route_recovery=start.options.auto_route.then_some(route_recovery);
            self.tun_last_error.lock().unwrap().take();
            drop(controls);
            info!(inbound_tag=%start.tag,name=%name,mtu=start.mtu,"TUN device started on its original Source Scope");
            Ok(())
        }.await;
        match result {
            Ok(()) => Ok(()),
            Err(source) => {
                shutdown.send_replace(true);
                completion.request_source_stop();
                let closed = completion.wait_main(deadline()).await;
                if completion.resources_complete().await {
                    let retired = {
                        let mut control = self.tun_control.lock().unwrap();
                        if control.as_ref().is_some_and(|control| {
                            control.id == id && control.budget_recovery.is_none()
                        }) {
                            control.take()
                        } else {
                            None
                        }
                    };
                    drop(retired);
                    clear_egress_after_route_cleanup(&self.egress_interface, true);
                    clear_matching_tun_info(self, id);
                }
                *self.tun_last_error.lock().unwrap() = Some(source.to_string());
                match closed {
                    Ok(()) => Err(source),
                    Err(cleanup) => Err(io::Error::other(StartFailure { source, cleanup }).into()),
                }
            }
        }
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
#[derive(Debug)]
struct StartFailure {
    source: EngineError,
    cleanup: io::Error,
}
impl std::fmt::Display for StartFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}; original System TUN cleanup: {}",
            self.source, self.cleanup
        )
    }
}
impl std::error::Error for StartFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
