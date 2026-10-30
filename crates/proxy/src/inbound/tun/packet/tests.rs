use std::net::IpAddr;

use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use zero_config::RuntimeConfig;
use zero_stack::{packet, packet_output::PacketSender};
use zero_traits::PacketBuffer;

use super::{try_forward, IcmpEchoRelay, InboundRouteRuntimeFactory, PacketSessionPins};
use crate::runtime::{route_runtime::SharedIngressRuntimeServices, Proxy};

mod boundaries;
#[cfg(feature = "wireguard")]
mod endpoints;
#[cfg(feature = "wireguard")]
mod groups;

struct Fixture {
    proxy: Proxy,
    route: InboundRouteRuntimeFactory,
    pins: PacketSessionPins,
    responses: PacketSender,
    received: mpsc::Receiver<Vec<u8>>,
    echo: IcmpEchoRelay,
    _shutdown: watch::Sender<bool>,
}

impl Fixture {
    fn new(value: Value) -> Self {
        let proxy = Proxy::new(config(value)).expect("build actual proxy");
        let route = InboundRouteRuntimeFactory::new(
            SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
            "tun-test".into(),
        );
        let (tx, received) = mpsc::channel::<Vec<u8>>(8);
        let responses: PacketSender = tx.into();
        let (_shutdown, shutdown) = watch::channel(false);
        let echo = IcmpEchoRelay::new(responses.clone(), route.clone(), shutdown);
        Self {
            pins: route.packet_statistics_pins(),
            proxy,
            route,
            responses,
            received,
            echo,
            _shutdown,
        }
    }

    async fn forward(&mut self, bytes: &[u8], mtu: usize, dns_hijack: bool) -> bool {
        let mut inner: PacketBuffer = bytes.to_vec().into();
        let handled = try_forward(
            &mut inner,
            &self.route,
            1,
            &mut self.pins,
            &self.responses,
            &self.echo,
            mtu,
            dns_hijack,
            None,
        )
        .await;
        assert_eq!(&*inner, bytes, "routing-only fixtures retain the packet");
        handled
    }

    fn no_response(&mut self) {
        assert!(matches!(
            self.received.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}

fn config(value: Value) -> RuntimeConfig {
    RuntimeConfig::parse(&value.to_string()).expect("parse fixture config")
}

fn direct() -> Value {
    json!({"route":{"rules":[],"final":{"type":"direct"}}})
}

fn reject_packet() -> Value {
    json!({"route":{"rules":[],"final":{"type":"reject"},"final_mode":"packet"}})
}

fn client(destination: IpAddr) -> IpAddr {
    if destination.is_ipv4() {
        "10.66.0.2".parse().unwrap()
    } else {
        "fd66::2".parse().unwrap()
    }
}

fn datagram(destination: IpAddr, port: u16, payload: &[u8]) -> Vec<u8> {
    packet::build_udp(client(destination), destination, 40000, port, payload)
}

fn syn(destination: IpAddr, port: u16) -> Vec<u8> {
    packet::build_tcp(
        client(destination),
        destination,
        40000,
        port,
        1,
        0,
        0x02,
        &[],
    )
}

#[tokio::test]
async fn warmed_flow_routes_observe_uncommitted_config_and_rollback() {
    let mut fixture = Fixture::new(direct());
    let packets = [
        syn("203.0.113.1".parse().unwrap(), 443),
        datagram("203.0.113.1".parse().unwrap(), 443, b"payload"),
        datagram("2001:db8::1".parse().unwrap(), 443, b"payload"),
    ];
    for _ in 0..3 {
        for packet in &packets {
            assert!(!fixture.forward(packet, 1500, false).await);
        }
    }
    let original = fixture.proxy.engine().runtime_snapshot();
    let revision = fixture.proxy.engine().config_revision();
    fixture
        .proxy
        .engine()
        .stage_runtime_config(config(reject_packet()))
        .unwrap();
    assert_eq!(fixture.proxy.engine().config_revision(), revision);
    for packet in &packets {
        assert!(fixture.forward(packet, 1500, false).await);
    }
    fixture
        .proxy
        .engine()
        .restore_staged_snapshot(original, false)
        .unwrap();
    assert_eq!(fixture.proxy.engine().config_revision(), revision);
    for packet in &packets {
        assert!(!fixture.forward(packet, 1500, false).await);
    }
    fixture
        .proxy
        .engine()
        .reload_runtime_config(config(reject_packet()))
        .unwrap();
    assert!(fixture.proxy.engine().config_revision() > revision);
    for packet in &packets {
        assert!(fixture.forward(packet, 1500, false).await);
    }
    fixture.no_response();
}
