//! Existing SS/HY2 production traffic after removal of all erased UDP resumes.
use crate::{
    runtime::{pipe::UdpPipeInput, udp_ingress::UdpIngressRuntime},
    Proxy,
};
use tokio::time::{timeout, Duration};
use zero_core::{Address, ProtocolType};

fn input(payload: &[u8]) -> UdpPipeInput<'_> {
    UdpPipeInput {
        peer_identity: None,
        target: Address::Ipv4([203, 0, 113, 1]),
        port: 443,
        payload,
        route_target: None,
        sniffed_original_target: None,
        sniffed_host_source: None,
        skip_fake_ip_restore: false,
        protocol: ProtocolType::new("test"),
        auth: None,
        source_addr: None,
        client_session_id: None,
        transparent_target: false,
        transparent_original_target: None,
        transparent_host_source: None,
    }
}
async fn exchange(proxy: &Proxy, payloads: &[&[u8]]) {
    let mut dispatch = UdpIngressRuntime::new(proxy.tcp_runtime_services())
        .new_dispatch("incoming")
        .await
        .unwrap();
    let mut previous = None;
    let mut total = 0;
    for payload in payloads {
        let id = dispatch.dispatch(input(payload)).await.unwrap();
        if let Some(previous) = previous {
            assert_eq!(id, previous);
        }
        previous = Some(id);
        let (_, tasks, _) = dispatch.poll_sockets();
        let packet = tasks.join_next_with_id().await.unwrap().unwrap().unwrap();
        assert_eq!(packet.0, Address::Ipv4([203, 0, 113, 1]));
        assert_eq!(packet.1, 443);
        assert_eq!(packet.2, *payload);
        crate::runtime::udp_delivery::record_chain_udp_response_parts(
            &proxy.udp_runtime_services(),
            &dispatch,
            packet.0,
            packet.1,
            packet.2,
            packet.3,
        );
        total += payload.len() as u64;
        let active = proxy
            .engine()
            .active_sessions()
            .into_iter()
            .find(|s| s.id == id)
            .unwrap();
        assert_eq!(active.outbound_tx_bytes, total);
        assert_eq!(active.outbound_rx_bytes, total);
    }
    dispatch.finish_all();
}
#[cfg(feature = "shadowsocks")]
#[tokio::test]
async fn shadowsocks_actual_udp_connection_preserves_two_packets_one_socket_and_flow_accounting() {
    timeout(Duration::from_secs(15),async {
        let peer=tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();let port=peer.local_addr().unwrap().port();
        let codec=::shadowsocks::transport::ShadowsocksTransportLeaf::new("peer","127.0.0.1",port,
            "chacha20-ietf-poly1305","typed-password").udp_packet_path_plan().unwrap().carrier_codec();
        let worker=tokio::spawn(async move {
            let mut original=None;let mut buffer=vec![0;65535];
            for expected in [b"first".as_slice(),b"second".as_slice()] {
                let (n,source)=peer.recv_from(&mut buffer).await.unwrap();
                if let Some(original)=original {assert_eq!(source,original);}original=Some(source);
                let (target,port,payload)=codec.decode(&buffer[..n]).unwrap();assert_eq!(payload,expected);
                let response=codec.encode(&target,port,&payload).unwrap();peer.send_to(&response,source).await.unwrap();
            }
        });
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[{"tag":"node","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":port,
                "cipher":"chacha20-ietf-poly1305","password":"typed-password"}}],
            "route":{"final":{"type":"route","outbound":"node"}}
        }).to_string()).unwrap();
        let proxy=Proxy::new(config).unwrap();exchange(&proxy,&[b"first",b"second"]).await;worker.await.unwrap();
    }).await.unwrap();
}
#[cfg(feature = "hysteria2")]
#[tokio::test]
async fn hysteria2_actual_udp_preserves_pre_sent_first_packet_reuse_tls_and_accounting() {
    use ::hysteria2::transport::{
        Hysteria2AuthenticatedInboundProfile, Hysteria2InboundBindOptionsRef,
        Hysteria2InboundBindPlan, Hysteria2InboundOptionsRef, Hysteria2InboundUserRef,
    };
    timeout(Duration::from_secs(20),async {
        let material=tempfile::tempdir().unwrap();let cert=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(material.path().join("ca.pem"),cert.cert.pem()).unwrap();
        std::fs::write(material.path().join("key.pem"),cert.signing_key.serialize_pem()).unwrap();
        let server=Hysteria2InboundBindPlan::from_options_refs(Some(material.path()),
            Hysteria2InboundBindOptionsRef{cert_path:Some("ca.pem"),key_path:Some("key.pem")})
            .bind("127.0.0.1:0").await.unwrap();let port=server.local_addr().unwrap().port();
        let (done,wait_done)=tokio::sync::oneshot::channel();
        let worker=tokio::spawn(async move {
            let connection=server.accept_connection().await.unwrap();
            let profile=Hysteria2AuthenticatedInboundProfile::from_options_refs(Hysteria2InboundOptionsRef {
                users:[Hysteria2InboundUserRef{password:"typed-password",principal_key:None,up_bps:None,down_bps:None,
                    device_limit:None,quota_remaining_bytes:None,policy_revision:None}],
            });
            let connection=profile.accept_authenticated_connection(connection).await.unwrap();
            let datagrams=connection.datagram_source();
            let mut session=::hysteria2::udp::Hysteria2InboundUdpSession::new();let mut original=None;
            for expected in [b"first".as_slice(),b"second".as_slice()] {
                let request=session.read_dispatch_parts_from_datagram(&datagrams).await.unwrap();
                let (target,port,payload,id)=request.into_pipe_parts();let id=id.unwrap();
                assert_eq!(payload,expected);if let Some(original)=original {assert_eq!(id,original);}original=Some(id);
                ::hysteria2::udp::Hysteria2InboundUdpCodec.send_datagram(&datagrams,id as u32,&target,port,&payload).unwrap();
            }
            wait_done.await.unwrap();connection.close("test complete");datagrams.closed().await;
        });
        let config=zero_config::RuntimeConfig::parse_with_source_dir(&serde_json::json!({
            "outbounds":[{"tag":"node","protocol":{"type":"hysteria2","server":"127.0.0.1","port":port,
                "password":"typed-password","server_name":"localhost","ca_cert_path":"ca.pem"}}],
            "route":{"final":{"type":"route","outbound":"node"}}
        }).to_string(),Some(material.path().to_owned())).unwrap();
        let proxy=Proxy::new(config).unwrap();exchange(&proxy,&[b"first",b"second"]).await;
        done.send(()).unwrap();worker.await.unwrap();
    }).await.unwrap();
}

#[cfg(feature = "hysteria2")]
#[tokio::test]
async fn typed_hysteria2_connection_reopens_after_actual_logical_end_without_resending_initial_packet(
) {
    use ::hysteria2::transport::{
        Hysteria2AuthenticatedInboundProfile, Hysteria2InboundBindOptionsRef,
        Hysteria2InboundBindPlan, Hysteria2InboundOptionsRef, Hysteria2InboundUserRef,
        Hysteria2ManagedUdpFlowConfig,
    };
    timeout(Duration::from_secs(25), async {
        let material = tempfile::tempdir().unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(material.path().join("ca.pem"), cert.cert.pem()).unwrap();
        std::fs::write(
            material.path().join("key.pem"),
            cert.signing_key.serialize_pem(),
        )
        .unwrap();
        let server = Hysteria2InboundBindPlan::from_options_refs(
            Some(material.path()),
            Hysteria2InboundBindOptionsRef {
                cert_path: Some("ca.pem"),
                key_path: Some("key.pem"),
            },
        )
        .bind("127.0.0.1:0")
        .await
        .unwrap();
        let port = server.local_addr().unwrap().port();
        let (finish, mut finished) = tokio::sync::mpsc::channel(1);
        let (closed, mut closures) = tokio::sync::mpsc::channel(1);
        let worker = tokio::spawn(async move {
            for expected in [b"first".as_slice(), b"second".as_slice()] {
                let connection = server.accept_connection().await.unwrap();
                let profile = Hysteria2AuthenticatedInboundProfile::from_options_refs(
                    Hysteria2InboundOptionsRef {
                        users: [Hysteria2InboundUserRef {
                            password: "typed-password",
                            principal_key: None,
                            up_bps: None,
                            down_bps: None,
                            device_limit: None,
                            quota_remaining_bytes: None,
                            policy_revision: None,
                        }],
                    },
                );
                let connection = profile
                    .accept_authenticated_connection(connection)
                    .await
                    .unwrap();
                let datagrams = connection.datagram_source();
                let mut session = ::hysteria2::udp::Hysteria2InboundUdpSession::new();
                let request = session
                    .read_dispatch_parts_from_datagram(&datagrams)
                    .await
                    .unwrap();
                let (target, port, payload, id) = request.into_pipe_parts();
                assert_eq!(payload, expected);
                ::hysteria2::udp::Hysteria2InboundUdpCodec
                    .send_datagram(&datagrams, id.unwrap() as u32, &target, port, &payload)
                    .unwrap();
                finished.recv().await.unwrap();
                connection.close("logical connection ended");
                datagrams.closed().await;
                closed.send(()).await.unwrap();
            }
        });
        let config =
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let plan =
            Hysteria2ManagedUdpFlowConfig::new("node", "127.0.0.1", port, "typed-password", None)
                .with_insecure(true)
                .flow_resume();
        let connection = super::datagram_manager::connection_from_plan(
            plan,
            crate::runtime::path::OutboundEndpoint {
                server: "127.0.0.1".into(),
                port,
            },
        );
        let mut tasks = tokio::task::JoinSet::new();
        for payload in [b"first".as_slice(), b"second".as_slice()] {
            assert_eq!(
                connection
                    .send_datagram(
                        &mut tasks,
                        Some(proxy.udp_runtime_services()),
                        77,
                        &Address::Ipv4([203, 0, 113, 1]),
                        443,
                        payload
                    )
                    .await
                    .unwrap(),
                payload.len()
            );
            let packet = tasks.join_next().await.unwrap().unwrap().unwrap();
            assert_eq!(packet.2, payload);
            finish.send(()).await.unwrap();
            closures.recv().await.unwrap();
            // Observe the protocol's actual queue retirement, never a delay or
            // a fabricated native-close report. This test qualifies logical
            // reconstruction only; original native admission gates stay closed.
            while !connection.is_closed() {
                tokio::task::yield_now().await;
            }
        }
        worker.await.unwrap();
    })
    .await
    .unwrap();
}

#[cfg(feature = "managed-stream-runtime")]
#[path = "tests/connections.rs"]
mod connections;
