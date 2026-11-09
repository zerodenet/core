//! One actual software Source and its separately owned real SOCKS callee.
use super::*;
use zero_platform_tokio::{
    contract::scope::TaskJoinWitness,
    controlled_carrier::{CarrierOriginObservation, CarrierOwnerRegistration},
};
pub(super) struct Client {
    pub proxy: Proxy,
    running: crate::RunningProxy,
    pub original: CarrierOrigin,
    physical: Vec<(CarrierOwnerRegistration, Vec<CarrierOriginObservation>)>,
    outer: Proxy,
    outer_running: crate::RunningProxy,
    outer_source: Arc<crate::runtime::inbound_owners::InboundOwner>,
}
impl Client {
    pub async fn start(peer: &peer::Peer, mut backend: zero_config::DnsServerConfig) -> Self {
        let outer_config=zero_config::RuntimeConfig::parse(r#"{"inbounds":[{"tag":"outer-in","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
        let outer = Proxy::new(outer_config.clone()).unwrap();
        let outer_running = outer.spawn();
        crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(outer.engine().clone()),
            outer.clone(),
        )
        .apply_runtime_config_and_wait(outer_config, Duration::from_secs(5))
        .await
        .unwrap();
        let outer_source = outer.inbound_owners.owner("outer-in").unwrap();
        outer_source.wait_ready().await.unwrap();
        let endpoint = *outer_source.original_listener_address.get().unwrap();
        match &mut backend {
            zero_config::DnsServerConfig::Udp { host, detour, .. }
            | zero_config::DnsServerConfig::Doh { host, detour, .. } => {
                *host = "10.0.0.2".into();
                *detour = Some("wg".into());
            }
            _ => unreachable!("existing UDP/DoH device consumers"),
        }
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"dns":{"servers":{"node":{"type":"system"},"query":backend},
                "default_server":"query","policy":{"node_server":"node","address_family":"ipv4_only"}}},
            "outbounds":[{"tag":"wg","protocol":{"type":"wireguard",
                "private_key":STANDARD.encode([121;32]),"addresses":["10.0.0.1/32"],"outer_udp_proxy":"outer",
                "peers":[{"public_key":peer::public(122),"endpoint":peer.address.to_string(),"allowed_ips":["0.0.0.0/0"]}]}},
                {"tag":"outer","protocol":{"type":"socks5","server":"127.0.0.1","port":endpoint.port()}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        proxy.resolver.set_outbound_connector(Arc::new(NoLegacy));
        let running = proxy.spawn();
        crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        )
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let selected = plan.configured("wg").unwrap();
        assert!(plan.has_device_carrier_provider(selected.role_id()));
        let mut origins = proxy
            .protocols
            .test_packet_source_origins(proxy.engine().config().as_ref());
        assert_eq!(origins.len(), 1);
        let original = origins.remove(0);
        assert_eq!(original.logical_packet().unwrap().drivers().len(), 1);
        assert!(original.closed().is_none() && original.packet_source_admission().is_none());
        plan.validate_carrier_origin(selected.role_id(), &original)
            .unwrap();
        let lower = plan.configured("outer").unwrap();
        let physical = (0..lower.recipe.spec().resources.len())
            .filter_map(|ordinal| lower.original_carrier_owner(ordinal))
            .map(|owner| {
                let observed = owner.observe().unwrap();
                let original = observed
                    .slots()
                    .iter()
                    .map(|slot| {
                        assert!(slot.native().is_some_and(|native| !native.is_closed()));
                        slot.original().clone()
                    })
                    .collect::<Vec<_>>();
                assert!(!original.is_empty());
                (owner, original)
            })
            .collect::<Vec<_>>();
        assert!(!physical.is_empty());
        Self {
            proxy,
            running,
            original,
            physical,
            outer,
            outer_running,
            outer_source,
        }
    }
    pub fn logical_pending(&self, actual: &zero_dns::OwnedDnsQueryObservation) {
        let child = actual.original.as_ref().unwrap();
        assert!(child
            .parent()
            .unwrap()
            .native_origin()
            .same_original(&self.original));
        assert!(child.closed().is_none() && self.original.closed().is_none());
        for driver in self.original.logical_packet().unwrap().drivers() {
            assert!(!driver.is_joined());
            assert!(!driver
                .scope_identity()
                .same_scope(&actual.task.scope_identity()));
        }
    }
    pub async fn close(self, children: &[CarrierOrigin], queries: &[TaskJoinWitness]) {
        assert!(self.proxy.engine().active_sessions().is_empty());
        assert!(self.proxy.engine().completed_sessions().is_empty());
        let devices = self
            .proxy
            .protocols
            .test_outbound_devices(self.proxy.engine().config().as_ref());
        assert_eq!(devices.len(), 1);
        let workers = devices[0].test_original_worker_witnesses();
        assert!(workers.len() >= 3);
        let source = self.original.logical_packet().unwrap();
        assert!(workers
            .iter()
            .all(|worker| worker.scope_identity().same_scope(source.device().scope())));
        let root = self.proxy.protocols.runtime_host().original_budgets();
        self.running.shutdown().await.unwrap();
        for query in queries {
            let joined = query.try_joined().unwrap();
            assert!(query.matches_joined(&joined) && joined.registration_released());
            joined.result().unwrap();
        }
        for worker in workers.iter().chain(source.drivers()) {
            let joined = worker.try_joined().unwrap();
            assert!(worker.matches_joined(&joined) && joined.registration_released());
            joined.result().unwrap();
        }
        for child in children {
            returned(child);
            assert!(child.owner().closed().unwrap().matches(child.owner()));
            assert!(child.owner().users_closed().unwrap().matches(child.owner()));
        }
        assert!(
            self.original.closed().is_none(),
            "software Scope ACK cannot mint the opaque physical ancestor's NativeClose"
        );
        for (owner, originals) in &self.physical {
            assert!(owner.closed().unwrap().matches(owner));
            assert!(owner.users_closed().unwrap().matches(owner));
            for original in originals {
                let receipt = original.closed().unwrap();
                assert!(original.matches_closed(&receipt) && receipt.had_native_acquisition());
            }
        }
        assert_eq!(
            root.carrier.snapshot().charged,
            1,
            "only the same opaque Source ancestor debt remains charged"
        );
        assert_eq!(root.packet.native().occupied.frames, 0);
        assert_eq!(root.packet.native().occupied.bytes, 0);
        self.outer_running.shutdown().await.unwrap();
        let terminal = self.outer_source.terminal_for_test().unwrap();
        assert_eq!(terminal.identity(), self.outer_source.identity());
        assert!(
            terminal.failure().is_none()
                && terminal.task_failures().is_empty()
                && terminal.native_bound_failures().is_empty()
        );
        assert!(terminal.closed_io_receipts() > 0 && terminal.joined_tasks() >= 1);
        assert!(self.outer.engine().active_sessions().is_empty());
    }
}
