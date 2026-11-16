//! Existing callback APIs exercise actual protocol roles and caller originals.
#![cfg(all(feature = "vmess", feature = "socks5"))]
use super::*;
use crate::{
    component::tests::business_carrier::sources::{Caller, Kind},
    Proxy,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    time::{timeout, Duration, Instant},
};
use zero_core::{component::*, request::DatagramTargets, Address, Network, ProtocolType, Session};
use zero_platform_tokio::contract::network::UpstreamTransport;
use zero_platform_tokio::{
    contract::{context::RequestIdentity, CancellationToken, CloseReason},
    controlled_carrier::{CarrierOriginObservation, CarrierOwnerRegistration},
};
struct Fixture {
    proxy: Proxy,
    caller: Caller,
    prefix: PrefixOpener,
    physical: CarrierOwnerRegistration,
}
impl Fixture {
    async fn new(endpoint: std::net::SocketAddr, kind: Kind, business: bool) -> Self {
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({"outbounds":[{"tag":"node","protocol":{"type":"socks5","server":"127.0.0.1","port":endpoint.port()}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        proxy
            .protocols
            .start_components(config.clone(), end)
            .await
            .unwrap();
        proxy
            .protocols
            .commit_components(&config, || Ok(()))
            .unwrap();
        let caller = Caller::new_for_session(
            &proxy,
            kind,
            Session::new(
                0,
                zero_core::Address::Ipv4([203, 0, 113, 9]),
                443,
                Network::Tcp,
                ProtocolType::new("prefix-original"),
            ),
        )
        .await;
        let services = proxy.tcp_runtime_services().execution();
        let plan = services.components.as_ref().unwrap();
        let slot = plan.configured("node").unwrap();
        let invocation = caller.invocation();
        let root = UpstreamBinding {
            prefix: vec![],
            dial: slot.dial.clone(),
            network_generation: zero_core::component::NetworkGeneration(
                services.upstream().network_generation(),
            ),
        };
        let waiting = super::super::super::pending::PendingCaller::new(
            &services,
            invocation,
            vec![slot.role_id()],
            root.clone(),
        )
        .unwrap();
        let admitted = crate::component::business_carrier::BusinessCarrierRequest::new(
            services.engine(),
            &slot,
            invocation,
        )
        .unwrap();
        let actual = super::super::super::network::NodeOpener::new(
            services.upstream_observed("node"),
            Destination::new(Address::Ipv4([127, 0, 0, 1]), endpoint.port()),
        )
        .with_plan(Some(Arc::downgrade(plan)))
        .with_business(admitted.clone())
        .with_pending(
            root.clone(),
            waiting
                .as_ref()
                .map(super::super::super::pending::PendingCaller::observer),
        );
        let cancel = invocation.cancellation();
        let binding = UpstreamBinding {
            prefix: vec![slot.role_id()],
            ..root.clone()
        };
        let target = Destination::new(Address::Ipv4([203, 0, 113, 9]), 443);
        let mut upstream = UpstreamTransport::new(root.clone(), Arc::new(actual));
        let identity = if business {
            invocation.identity.clone()
        } else {
            upstream = upstream
                .select_background_carrier_observed(
                    Some(root),
                    CarrierContext {
                        session: None,
                        deadline: None,
                        cancel: cancel.clone(),
                    },
                    waiting.as_ref().unwrap().observer(),
                )
                .await
                .unwrap();
            RequestIdentity::Service(
                Arc::new(zero_core::request::ServiceRequestFacts {
                    origin: zero_core::request::ServiceOrigin::Runtime,
                    purpose: zero_core::request::ServicePurpose::BackgroundControl,
                })
                .into(),
            )
        };
        let prefix = PrefixOpener {
            service_identity: None,
            role: slot.stream(end).await.unwrap(),
            business: business.then(|| admitted.unwrap().with_prefix(&binding.prefix)),
            plan: Some(Arc::downgrade(plan)),
            datagram_slot: Some(crate::inventory::StreamRoleRef::new(&slot)),
            context: OpenContext {
                identity,
                deadline: None,
                cancel,
                upstream,
                observer: None,
            },
            expected_binding: binding,
            expected_endpoint: target.clone(),
            request_destination: Destination::new(Address::Domain("203.0.113.9".into()), 443),
            network: services.upstream(),
            observer: None,
            health: None,
        };
        let physical = slot.original_carrier_owner(0).unwrap();
        Self {
            proxy,
            caller,
            prefix,
            physical,
        }
    }
    fn call(&self) -> CarrierContext {
        CarrierContext {
            session: self.prefix.context.identity.session(),
            deadline: None,
            cancel: CancellationToken::new(),
        }
    }
    fn originals(&self) -> Vec<CarrierOriginObservation> {
        self.physical
            .observe()
            .unwrap()
            .slots()
            .iter()
            .map(|row| row.original().clone())
            .collect()
    }
    async fn finish(mut self, originals: Vec<CarrierOriginObservation>) {
        self.caller.join().await;
        assert!(self.caller.witness().is_joined());
        self.caller.verify_carrier_completion();
        self.proxy
            .engine()
            .track_session(self.caller.session().id)
            .finish(zero_engine::SessionOutcome::Cancelled)
            .unwrap();
        self.proxy
            .protocols
            .shutdown_components(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        for original in originals {
            let closed = original
                .closed()
                .expect("same callback donor original close");
            assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
        }
        assert_eq!(self.physical.observe().unwrap().retained(), 0);
        assert_eq!(
            self.proxy
                .protocols
                .runtime_host()
                .original_budgets()
                .carrier
                .snapshot()
                .charged,
            0
        );
    }
}
#[path = "relay_prefix/datagram.rs"]
mod datagram_tests;
#[path = "relay_prefix/stream.rs"]
mod stream_tests;
