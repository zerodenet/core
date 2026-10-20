//! Real protocol codec + real owned UDP root + neutral codec/observer layers.
use super::*;
use crate::runtime::udp_dispatch::packet_owner::{PacketCarrierDemand, PacketCarrierOwnerSlot};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use zero_core::{
    component::NetworkGeneration,
    request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::contract::{
    context::RequestIdentity,
    network::{CarrierContext, UpstreamBinding},
};

#[derive(Debug, Default)]
struct Meter(AtomicUsize);
impl zero_traits::IoObserver for Meter {
    fn received(&self, bytes: usize) {
        self.0.fetch_add(bytes, Ordering::Relaxed);
    }
    fn sent(&self, bytes: usize) {
        self.0.fetch_add(bytes, Ordering::Relaxed);
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
#[tokio::test]
async fn original_protocol_owned_root_survives_neutral_wrappers_and_closes_same_reservation() {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[{"tag":"node", "protocol":{"type":"shadowsocks", "server":"127.0.0.1", "port":peer.local_addr().unwrap().port(), "cipher":"chacha20-ietf-poly1305", "password":"owned-fixture"}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let leaf =
        super::super::transport_leaf(&Default::default(), "node", &config.outbounds[0].protocol)
            .unwrap();
    let plan = leaf.udp_packet_path_plan().unwrap();
    let codec = plan.carrier_codec();
    let operation = ShadowsocksPacketPathOperation { plan: plan.clone() };
    let proxy = crate::Proxy::new(config).unwrap();
    let services = crate::protocol_registry::PacketPathExecutionServices::from_tcp_execution(
        &proxy.tcp_runtime_services().execution(),
    );
    let slot = PacketCarrierOwnerSlot::new();
    let demand = PacketCarrierDemand {
        identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::DiagnosticProbe,
        })),
        context: CarrierContext {
            session: None,
            cancel: zero_platform_tokio::contract::CancellationToken::new(),
            deadline: Some(tokio::time::Instant::now() + Duration::from_secs(3)),
        },
        binding: UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(services.network().egress_generation()),
        },
    };
    let path = operation
        .build_owned_carrier(services, slot.clone(), demand)
        .await
        .unwrap();
    let meter = Arc::new(Meter::default());
    let path = crate::runtime::udp_flow::packet_path_chain::carriers::observed::wrap(
        path,
        Some(meter.clone()),
    );
    let path = crate::runtime::udp_flow::packet_path_chain::carriers::encoded::wrap(
        path,
        packet_path_datagram_source(plan),
    );
    assert!(Arc::ptr_eq(&path.native_owner().unwrap(), &slot));
    let target = Address::Ipv4([192, 0, 2, 5]);
    path.send_to(&target, 53, b"request").await.unwrap();
    let mut wire = [0; 65536];
    let (length, sender) = peer.recv_from(&mut wire).await.unwrap();
    let (_, _, inner) = codec.decode(&wire[..length]).unwrap();
    assert_eq!(
        codec.decode(&inner).unwrap(),
        (target.clone(), 53, b"request".to_vec())
    );
    assert!(meter.0.load(Ordering::Relaxed) > 0);
    let inner = codec.encode(&target, 53, b"reply").unwrap();
    let outer = codec
        .encode(
            &Address::Ipv4([127, 0, 0, 1]),
            peer.local_addr().unwrap().port(),
            &inner,
        )
        .unwrap();
    peer.send_to(&outer, sender).await.unwrap();
    let mut output = [0; 32];
    assert_eq!(
        path.recv_from_with_source(&mut output).await.unwrap(),
        (5, Some("192.0.2.5:53".parse().unwrap()))
    );
    assert_eq!(&output[..5], b"reply");
    let report = path.native_owner().unwrap().close_native();
    assert!(report.is_complete());
    assert_eq!(report.native.confirmed_closed, 1);
    assert!(path.send_to(&target, 53, b"after-close").await.is_err());
}
