use super::*;
use zero_platform_tokio::contract::Reason;
use zero_transport::quic::QuicInbound;

#[tokio::test]
async fn hysteria2_cancelled_actual_pending_dial_is_cleaned_before_successful_same_role_reopen() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    tokio::time::timeout(Duration::from_secs(20), async {
        let material = tempfile::tempdir().unwrap();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(material.path().join("cert.pem"), certificate.cert.pem()).unwrap();
        std::fs::write(material.path().join("key.pem"), certificate.signing_key.serialize_pem()).unwrap();
        let peer = QuicInbound::bind("127.0.0.1:0", "cert.pem", "key.pem", Some(material.path()), &[b"h3".to_vec()]).await.unwrap();
        let port = peer.local_addr().unwrap().port();
        let proxy = Proxy::new(zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[{"tag":"hy","protocol":{"type":"hysteria2","server":"127.0.0.1",
                "server_name":"localhost","port":port,"password":"p","insecure":true}}],
            "route":{"final":{"type":"route","outbound":"hy"}}
        }).to_string()).unwrap()).unwrap();
        start(&proxy).await;
        let services = proxy.tcp_runtime_services().execution();
        let slot = services.components.as_ref().unwrap().configured("hy").unwrap();
        let role = slot.stream(end()).await.unwrap();
        let context = |cancel| OpenContext {
            identity: RequestIdentity::Service(Arc::new(zero_core::request::ServiceRequestFacts {
                origin: zero_core::request::ServiceOrigin::Runtime,
                purpose: zero_core::request::ServicePurpose::DiagnosticProbe,
            }).into()),
            deadline: Some(end()), cancel, observer: None,
            upstream: UpstreamTransport::new(UpstreamBinding {
                prefix: vec![], dial: slot.dial.clone(),
                network_generation: zero_core::component::NetworkGeneration(proxy.egress_interface.generation()),
            }, Arc::new(Denied)),
        };
        let request = ConnectRequest {destination: zero_core::request::Destination::new(Address::Ipv4([203,0,113,5]),443)};
        let cancel = CancellationToken::new();
        let mut pending = Box::pin(role.connect(request.clone(), context(cancel.clone())));
        let held_incoming = tokio::select! {
            actual = peer.accept_incoming() => actual.unwrap(),
            result = &mut pending => panic!("dial completed before the real peer handshake was driven: {}",result.is_ok()),
        };
        poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending(), "a real QUIC Initial was delivered while the original handshake remains pending");
            Poll::Ready(())
        }).await;
        cancel.cancel();
        let failure = match pending.as_mut().await {
            Err(error) => error,
            Ok(_) => panic!("cancelled original dial returned a business stream"),
        };
        assert_eq!(failure.reason, Reason::Cancelled);
        drop(pending);
        let authenticated = Arc::new(AtomicUsize::new(0));
        let count = authenticated.clone();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            let incoming = peer.accept_incoming().await.unwrap();
            let raw = QuicInbound::establish_incoming(incoming).await.unwrap();
            let profile=Hysteria2AuthenticatedInboundProfile::from_options_refs(Hysteria2InboundOptionsRef {
                users:[Hysteria2InboundUserRef {password:"p",principal_key:None,up_bps:None,down_bps:None,device_limit:None,quota_remaining_bytes:None,policy_revision:None}],
            });
            let connection = profile.accept_authenticated_connection(raw).await.unwrap();
            count.fetch_add(1,Ordering::SeqCst);
            let (_,mut stream) = connection.accept_next_tcp_stream().await.unwrap().unwrap();
            connection.response_protocol().send_ok(&mut stream).await.unwrap();
            let mut payload = [0;4];stream.read_exact(&mut payload).await.unwrap();assert_eq!(&payload,b"ping");
            stream.write_all(&payload).await.unwrap();stream.shutdown().await.unwrap();drop(stream);
            finished.await.unwrap();
            connection.datagram_source().close(0x100u32.into(),b"pending dial fixture finished");connection.datagram_source().closed().await;
        });
        let connected = proxy.protocols.drive_component_supervision(role.connect(request,context(CancellationToken::new()))).await.unwrap();
        let mut stream = connected.io;
        stream.write_all(b"ping").await.unwrap();
        let mut payload = [0;4];stream.read_exact(&mut payload).await.unwrap();assert_eq!(&payload,b"ping");
        assert_eq!(authenticated.load(Ordering::SeqCst),1,"cancelled pending state cannot become a second successful authentication");
        // This incoming remains undriven until the replacement has genuinely
        // succeeded. Peer ACK, timeout or the port being reusable is not used
        // as a substitute for the old pool owner's cleanup chain.
        drop(held_incoming);
        stream.shutdown().await.unwrap();drop(stream);
        finish.send(()).unwrap();worker.await.unwrap();
        proxy.protocols.shutdown_components(end()).await.unwrap();
    }).await.unwrap();
}
