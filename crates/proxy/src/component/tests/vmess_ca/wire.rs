use super::fixtures::*;
use std::sync::{atomic::Ordering, Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::{
    context::ConnectFact,
    stream::{ConnectRequest, PeerConfirmation},
};
use zero_transport::profile::{OwnedGrpcProfile, OwnedWebSocketProfile};

#[tokio::test]
async fn actual_native_file_grant_prepares_vmess_tls_ws_and_grpc_without_reopening_ca_at_connect() {
    for carrier in [0, 1, 2] {
        timeout(Duration::from_secs(5), async {
            let directory = Directory::new();
            let path = directory.0.join("identity.pem");
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (acceptor, pem) = tls_server(carrier == 2);
            let ws = (carrier == 1).then(|| OwnedWebSocketProfile {
                accept_proxy_protocol: false,
                heartbeat_period_secs: 0,
                host: Some("native.example".into()),
                path: "/trusted-vmess?ed=4096".into(),
                headers: vec![("x-file-grant".into(), "preserved".into())],
            });
            let grpc = (carrier == 2).then(|| OwnedGrpcProfile {
                service_names: vec!["ca.vmess/Tun".into()],
                authority: Some("native.example".into()),
                ..Default::default()
            });
            let mut tls = tls_profile();
            if carrier == 2 {
                tls.alpn = vec!["h2".into()];
            }
            let options = options(
                address,
                &tls,
                ws.as_ref(),
                grpc.as_ref(),
                &directory.0,
                pem.len(),
            );
            let (mut instance, mut scope, grants) = build(options, &path, pem.len());
            let observations = Arc::new(Observations::default());
            assert!(instance
                .roles
                .stream
                .as_ref()
                .unwrap()
                .connect(
                    ConnectRequest {
                        destination: destination()
                    },
                    context(address, observations.clone())
                )
                .await
                .is_err());
            assert_eq!(observations.dials.load(Ordering::Relaxed), 0);
            // The CA did not exist during describe/build. Only start reads it.
            std::fs::write(&path, &pem).unwrap();
            let access = grants.activate(deadline()).unwrap();
            instance.lifecycle.start(deadline()).await.unwrap();
            drop(access);
            std::fs::remove_file(&path).unwrap(); // Connect must use its authorized snapshot.
            let server = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                let tls = acceptor.accept(io).await.unwrap();
                assert_eq!(
                    tls.get_ref().1.alpn_protocol(),
                    (carrier == 2).then_some(b"h2".as_slice())
                );
                let carrier = if let Some(ws) = ws {
                    zero_platform_tokio::TcpRelayStream::new(
                        zero_transport::ws::accept_ws_profile(tls, &ws)
                            .await
                            .unwrap(),
                    )
                } else if let Some(grpc) = grpc {
                    zero_platform_tokio::TcpRelayStream::new(
                        zero_transport::grpc::accept_grpc_with_profile(tls, &grpc)
                            .await
                            .unwrap(),
                    )
                } else {
                    zero_platform_tokio::TcpRelayStream::new(tls)
                };
                let (facts, mut io) = inbound()
                    .accept_tcp_stream(vmess::inbound::VmessInbound, carrier)
                    .await
                    .unwrap();
                assert_eq!(facts.target, destination().address);
                assert_eq!(facts.port, destination().port);
                let mut payload = [0; 7];
                io.read_exact(&mut payload).await.unwrap();
                assert_eq!(&payload, b"payload");
                io.write_all(b"trusted-echo").await.unwrap();
                io.flush().await.unwrap();
                let mut end = [0];
                assert_eq!(io.read(&mut end).await.unwrap(), 0);
            });
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
            assert_eq!(
                *observations.facts.lock().unwrap(),
                vec![
                    (None, ConnectFact::CarrierReady),
                    (None, ConnectFact::RequestSubmitted)
                ]
            );
            assert!(observations.outer.load(Ordering::Relaxed) > REQUEST_LEN);
            connected.io.write_all(b"payload").await.unwrap();
            connected.io.flush().await.unwrap();
            let mut echo = [0; 12];
            connected.io.read_exact(&mut echo).await.unwrap();
            assert_eq!(&echo, b"trusted-echo");
            assert_eq!(
                observations.facts.lock().unwrap().last(),
                Some(&(None, ConnectFact::PeerConfirmed))
            );
            connected.io.shutdown().await.unwrap();
            server.await.unwrap();
            drop(connected);
            instance.lifecycle.shutdown(deadline()).await.unwrap();
            assert!(scope.shutdown(deadline()).await.is_complete());
            let report = grants.shutdown_io(deadline()).await.unwrap();
            assert!(report.is_complete());
            assert_eq!(report.released_resources.len(), 1);
        })
        .await
        .unwrap();
    }
}
