//! Published association retirement still waits its two actual unique views.
use super::*;
use tokio::net::UdpSocket;
use zero_core::packet::Datagram;
use zero_platform_tokio::contract::{
    datagram::{recv, send},
    CloseReason,
};

#[tokio::test]
async fn dropped_published_socks_service_pair_keeps_views_charged_then_closes_both_originals_without_an_observer(
) {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Destination::new(Address::Ipv4([127,0,0,1]), listener.local_addr().unwrap().port());
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let relay = socket.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (tcp,_) = listener.accept().await.unwrap();
            let mut tcp = zero_platform_tokio::TokioSocket::new(tcp);
            assert!(matches!(socks5::Socks5Inbound.accept_command(&mut tcp).await.unwrap(), socks5::Socks5Request::UdpAssociate(_)));
            socks5::Socks5Inbound.send_success_response_with_bound(&mut tcp, &Address::Ipv4([127,0,0,1]), relay.port()).await.unwrap();
            let mut tcp = tcp.into_inner().unwrap();
            let codec = socks5::udp::Socks5InboundUdpCodec;
            let mut bytes = vec![0;65535];
            for expected in [b"".as_slice(), b"first", b"", b"last", b""] {
                let (size,source) = socket.recv_from(&mut bytes).await.unwrap();
                let request = codec.decode_request(&bytes[..size]).unwrap();
                assert_eq!(request.payload(), expected);
                let response = codec.encode_response_to_client(request.target(), request.port(), request.payload()).unwrap();
                socket.send_to(&response, source).await.unwrap();
            }
            let mut extra = Vec::new();
            tcp.read_to_end(&mut extra).await.unwrap();
            assert!(extra.is_empty());
            tcp.shutdown().await.unwrap();
        });
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"native_stream_max_originals":2},
            "outbounds":[{"tag":"node","protocol":{"type":"socks5","server":"127.0.0.1","port":endpoint.port}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(config, Duration::from_secs(3)).await.unwrap();
        let services = proxy.tcp_runtime_services().execution();
        let plan = services.components.as_ref().unwrap().clone();
        let slot = plan.configured("node").unwrap();
        let physical = slot.original_carrier_owner(0).unwrap();
        let end = Instant::now() + Duration::from_secs(3);
        let role = slot.datagram(end).await.unwrap();
        let cancel = CancellationToken::new();
        let call = CarrierContext { session:None, deadline:Some(end), cancel:cancel.clone() };
        let binding = UpstreamBinding { prefix:vec![], dial:Default::default(), network_generation:NetworkGeneration(services.upstream().network_generation()) };
        let opener = crate::component::stream_bridge::network::NodeOpener::new(services.upstream_observed("node"), endpoint)
            .with_plan(Some(Arc::downgrade(&plan))).with_service_call(call);
        let context = OpenContext {
            identity:RequestIdentity::Service(Arc::new(ServiceRequestFacts { origin:ServiceOrigin::Runtime, purpose:ServicePurpose::DiagnosticProbe }).into()),
            deadline:Some(end), cancel:cancel.clone(), observer:None,
            upstream:UpstreamTransport::new(binding, Arc::new(opener)),
        };
        assert!(context.upstream.observed_pending_carrier().is_none());
        let mut channel = role.open(DatagramRequest { association:AssociationId(72), targets:DatagramTargets::Multiple }, context).await.unwrap();
        let actual = physical.observe().unwrap();
        assert_eq!(actual.slots().len(), 2);
        for slot in actual.slots() {
            assert!(slot.native().is_some() && slot.physical_view_live());
            assert!(slot.driver().is_none() && slot.auxiliary().is_none());
        }
        let originals = [actual.slots()[0].original().clone(), actual.slots()[1].original().clone()];
        for (index, payload) in [b"".as_slice(), b"first", b"", b"last", b""].into_iter().enumerate() {
            let target = if index % 2 == 0 { Address::Domain("actual.wire.target".into()) } else { Address::Ipv4([203,0,113,9]) };
            let frame = Datagram { association:AssociationId(72), address:Destination::new(target,53), payload:payload.to_vec().into() };
            let receipt = send(channel.tx.as_mut(), channel.control.as_mut(), &frame, &channel.limits).await.unwrap();
            assert_eq!(receipt.accepted_bytes, payload.len());
            let response = recv(channel.rx.as_mut()).await.unwrap().unwrap();
            assert_eq!(response.association, frame.association);
            assert_eq!(response.address, frame.address);
            assert_eq!(response.payload.as_ref(), payload);
        }
        channel.control.cancel(CloseReason::Shutdown);
        tokio::task::yield_now().await;
        assert!(originals.iter().all(|original| original.closed().is_none()), "retained unique views must still hold both originals");
        assert_eq!(physical.observe().unwrap().slots().len(), 2);
        drop(channel);
        assert!(!cancel.is_cancelled(), "only the association retirement fence was requested");
        loop {
            if originals.iter().all(|original| original.closed().is_some()) && physical.observe().unwrap().slots().is_empty() { break; }
            tokio::task::yield_now().await;
        }
        for original in &originals {
            let closed = original.closed().unwrap();
            assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
        }
        peer.await.unwrap();
        drop(role);
        running.shutdown().await.unwrap();
        assert!(physical.closed().unwrap().matches(&physical));
        assert!(physical.users_closed().unwrap().matches(&physical));
    }).await.unwrap();
}
