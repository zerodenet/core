use super::support::{ip, target, Fixture, LOCAL, REMOTE};
use crate::runtime::udp_flow::managed::{packet::PacketUdpFlow, ManagedTupleUdpFlowConnection};
use std::time::Duration;
use zero_stack::{packet, FragmentOutcome, FragmentReassembler};

#[tokio::test]
async fn pure_packet_endpoint_preserves_udp_mtu_reply_and_shared_lifetime() {
    let mut fixture = Fixture::new(256);
    let (destination, binding) = fixture.adapter.select(&[target()], 0).unwrap();
    let flow = PacketUdpFlow::open(
        binding.endpoint,
        super::support::address(ip(REMOTE)),
        443,
        destination,
        binding.local_ip,
        None,
    )
    .unwrap();
    let mut responses = flow.subscribe_responses();
    let payload = vec![7; 1024];
    flow.send(&super::support::address(ip(REMOTE)), 443, &payload)
        .await
        .unwrap();
    let mut reassembly = FragmentReassembler::new();
    let request = loop {
        let outgoing = fixture.packets.recv().await.unwrap();
        assert!(outgoing.packet.len() <= 256);
        match reassembly.process(&outgoing.packet, std::time::Instant::now()) {
            FragmentOutcome::Reassembled(packet) => break packet,
            FragmentOutcome::Pending => {}
            _ => panic!("unexpected fragment result"),
        }
    };
    let request = packet::parse_udp(&request).unwrap();
    assert_eq!(request.payload, payload);
    assert_eq!(request.src.ip, ip(LOCAL));
    let reply = packet::build_udp(ip(REMOTE), ip(LOCAL), 443, request.src.port, b"reply");
    assert!(fixture
        .device
        .deliver_decrypted_owned(reply.into(), false)
        .await
        .is_ok());
    let (address, port, payload) = tokio::time::timeout(Duration::from_secs(1), responses.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (address, port, payload),
        (super::support::address(ip(REMOTE)), 443, b"reply".to_vec())
    );
    drop(flow);
    assert!(!fixture.device.is_closed());
    let (_, binding) = fixture.adapter.select(&[target()], 0).unwrap();
    let second = PacketUdpFlow::open(
        binding.endpoint,
        super::support::address(ip(REMOTE)),
        443,
        destination,
        binding.local_ip,
        None,
    )
    .unwrap();
    fixture.device.close_now();
    assert!(second.is_closed());
    assert!(second
        .send(&super::support::address(ip(REMOTE)), 443, b"closed")
        .await
        .is_err());
}

#[tokio::test]
async fn generic_udp_operation_starts_a_managed_flow_on_a_pure_packet_endpoint() {
    use crate::protocol_registry::{UdpAdapterContext, UdpRuntimeServices};
    use crate::runtime::{udp_flow::result::FlowStartResult, udp_ingress::UdpIngressRuntime};
    use zero_core::{Network, ProtocolType, Session};
    let mut fixture = Fixture::new(1420);
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = crate::runtime::Proxy::new(config.clone()).unwrap();
    let services = proxy.tcp_runtime_services();
    let mut dispatch = UdpIngressRuntime::new(services.clone())
        .new_dispatch("test-in")
        .await
        .unwrap();
    let context = UdpAdapterContext::new(&config, UdpRuntimeServices::new(services));
    let session = Session::new(
        1,
        super::support::address(ip(REMOTE)),
        443,
        Network::Udp,
        ProtocolType::UNKNOWN,
    );
    let result = fixture
        .adapter
        .prepare_udp()
        .execute(&mut dispatch, context, &session, b"request")
        .await
        .unwrap_or_else(|error| panic!("{}: {}", error.stage, error.error));
    assert!(matches!(result, FlowStartResult::Flow { tx_bytes: 7, .. }));
    let outgoing = fixture.packets.recv().await.unwrap();
    assert_eq!(
        packet::parse_udp(&outgoing.packet).unwrap().payload,
        b"request"
    );
    drop(dispatch);
    assert!(!fixture.device.is_closed());
}

#[tokio::test]
async fn cancelling_udp_flow_releases_worker_under_packet_backpressure() {
    let fixture = Fixture::new(1420);
    let (destination, binding) = fixture.adapter.select(&[target()], 0).unwrap();
    let flow = PacketUdpFlow::open(
        binding.endpoint,
        super::support::address(ip(REMOTE)),
        443,
        destination,
        binding.local_ip,
        None,
    )
    .unwrap();
    let mut responses = flow.subscribe_responses();
    // Endpoint queue and stack queue are both bounded to 128 packets.
    let saturate = async {
        for _ in 0..512 {
            flow.send(&super::support::address(ip(REMOTE)), 443, b"pressure")
                .await
                .unwrap();
        }
    };
    assert!(tokio::time::timeout(Duration::from_millis(100), saturate)
        .await
        .is_err());
    drop(flow);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), responses.recv())
            .await
            .unwrap()
            .is_err()
    );
    assert!(!fixture.device.is_closed());
}
