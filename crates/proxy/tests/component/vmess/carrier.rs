use super::fixtures::*;
use std::sync::{atomic::Ordering, Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::stream::{ConnectRequest, PeerConfirmation};
use zero_transport::profile::{OwnedGrpcProfile, OwnedWebSocketProfile};

#[tokio::test]
async fn real_native_vmess_tls_websocket_and_grpc_keep_profiles_and_distinct_protocol_counts() {
    for carrier in [0, 1, 2, 3] {
        let grpc_carrier = carrier == 2;
        timeout(Duration::from_secs(5), async {
            let ws = matches!(carrier, 1 | 3).then(|| OwnedWebSocketProfile {
                accept_proxy_protocol: false,
                heartbeat_period_secs: 0,
                host: Some("native.example".into()),
                path: if carrier == 3 {
                    "/native-vmess?ed=4096"
                } else {
                    "/native-vmess"
                }
                .into(),
                headers: vec![("x-native-profile".into(), "preserved".into())],
            });
            let grpc = grpc_carrier.then(|| OwnedGrpcProfile {
                service_names: vec!["native.vmess/Tun".into()],
                authority: Some("native.example".into()),
                ..Default::default()
            });
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let acceptor = acceptor_with_alpn(grpc_carrier);
            let server_ws = ws.clone();
            let server_grpc = grpc.clone();
            let server = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                let tls = acceptor.accept(io).await.unwrap();
                assert_eq!(
                    tls.get_ref().1.alpn_protocol(),
                    grpc_carrier.then_some(b"h2".as_slice())
                );
                let carrier = if let Some(ws) = server_ws {
                    zero_platform_tokio::TcpRelayStream::new(
                        zero_transport::ws::accept_ws_profile(tls, &ws)
                            .await
                            .unwrap(),
                    )
                } else if let Some(grpc) = server_grpc {
                    zero_platform_tokio::TcpRelayStream::new(
                        zero_transport::grpc::accept_grpc_with_profile(tls, &grpc)
                            .await
                            .unwrap(),
                    )
                } else {
                    zero_platform_tokio::TcpRelayStream::new(tls)
                };
                let (facts, mut stream) = profile("aes-128-gcm")
                    .accept_tcp_stream(vmess::inbound::VmessInbound, carrier)
                    .await
                    .unwrap();
                assert_eq!(facts.target, destination().address);
                assert_eq!(facts.port, destination().port);
                let mut payload = [0; 7];
                stream.read_exact(&mut payload).await.unwrap();
                assert_eq!(&payload, b"payload");
                stream.write_all(b"carrier-echo").await.unwrap();
                stream.flush().await.unwrap();
                // Keep the real gRPC driver alive until the client receives it.
                let mut byte = [0];
                let _ = stream.read(&mut byte).await;
            });
            let observations = Arc::new(Observations::default());
            let mut tls = tls_profile();
            if grpc_carrier {
                tls.alpn = vec!["h2".into()];
            }
            let (mut instance, mut scope) = instance(
                address,
                "aes-128-gcm",
                Some(&tls),
                ws.as_ref(),
                grpc.as_ref(),
            );
            instance
                .lifecycle
                .start(super::super::fixtures::deadline())
                .await
                .unwrap();
            let mut connected = instance
                .roles
                .stream
                .as_ref()
                .unwrap()
                .connect(
                    ConnectRequest {
                        destination: destination(),
                    },
                    context(address, observations.clone()),
                )
                .await
                .unwrap();
            assert_eq!(connected.peer_confirmation, PeerConfirmation::Deferred);
            assert_eq!(
                *observations.protocol.lock().unwrap(),
                Some((0, REQUEST_LEN))
            );
            assert!(observations.outer.load(Ordering::Relaxed) > REQUEST_LEN);
            connected.io.write_all(b"payload").await.unwrap();
            connected.io.flush().await.unwrap();
            let mut echoed = [0; 12];
            connected.io.read_exact(&mut echoed).await.unwrap();
            assert_eq!(&echoed, b"carrier-echo");
            assert_eq!(
                observations.facts.lock().unwrap().last(),
                Some(&(
                    None,
                    zero_platform_tokio::contract::context::ConnectFact::PeerConfirmed
                ))
            );
            drop(connected);
            server.await.unwrap();
            instance
                .lifecycle
                .shutdown(super::super::fixtures::deadline())
                .await
                .unwrap();
            assert!(scope
                .shutdown(super::super::fixtures::deadline())
                .await
                .is_complete());
        })
        .await
        .unwrap();
    }
}
