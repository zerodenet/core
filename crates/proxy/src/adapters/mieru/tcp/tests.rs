use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;
use crate::Proxy;
use std::sync::{
    atomic::{AtomicI32, AtomicUsize, Ordering},
    Arc,
};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{EngineError, ResolvedOutbound, RouteDecision};

#[tokio::test]
async fn pooled_tcp_relay_keeps_original_carrier_io_error_without_reopening() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let config = zero_config::RuntimeConfig::parse(r#"{
            "outbounds":[{"tag":"pool","protocol":{"type":"mieru",
                "server":"127.0.0.1","port":443,"username":"test-user","password":"test-password"}}],
            "route":{"final":{"type":"route","outbound":"pool"}}
        }"#).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let snapshot = proxy.engine().runtime_snapshot();
        let (resolved, _) = proxy.engine().resolve_route_decision(RouteDecision::Route("pool".into())).unwrap();
        let ResolvedOutbound::Single(leaf) = resolved else { panic!("actual pool leaf") };
        let claimed = proxy.protocols.claim_outbound_leaf(snapshot.config(), leaf).unwrap();
        let (_, _, operation) = claimed.tcp_relay_hop().unwrap();
        let mut session = Session::new(0, Address::Ipv4([203,0,113,1]), 443, Network::Tcp, ProtocolType::new("test"));
        proxy.engine().prepare_session(&mut session,"incoming").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let native_code = Arc::new(AtomicI32::new(0));
        let attempts = calls.clone();
        let recorded = native_code.clone();
        let carrier = LazyTcpRelayCarrier::new("actual-failed-prefix".into(),0,Box::pin(async move {
            attempts.fetch_add(1,Ordering::SeqCst);
            // Port zero cannot be a listening peer. This obtains a real OS
            // connection failure rather than fabricating a successful probe.
            let error = tokio::net::TcpStream::connect(("127.0.0.1",0)).await.unwrap_err();
            recorded.store(error.raw_os_error().expect("native source"),Ordering::SeqCst);
            Err(EngineError::Io(error))
        }));
        let services = proxy.tcp_runtime_services().execution().upstream_observed("pool");
        let error = match operation.execute_lazy(services,carrier,&session).await {
            Err(error) => error,
            Ok(_) => panic!("actual failed carrier must not establish a stream"),
        };
        assert_eq!(calls.load(Ordering::SeqCst),1);
        let expected = native_code.load(Ordering::SeqCst);
        assert_ne!(expected,0);
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        let mut found = false;
        while let Some(error) = cause {
            let io = error.downcast_ref::<std::io::Error>()
                .or_else(|| match error.downcast_ref::<EngineError>() {
                    Some(EngineError::Io(error)) => Some(error),
                    _ => None,
                })
                .or_else(|| match error.downcast_ref::<zero_transport::RuntimeError>() {
                    Some(zero_transport::RuntimeError::Io(error)) => Some(error),
                    _ => None,
                });
            if io.is_some_and(|error| error.raw_os_error() == Some(expected)) {
                found = true;
                break;
            }
            // io::Error and the transparent domain enums may skip their boxed
            // payload in Error::source; inspect the retained typed payload too.
            cause = io.and_then(|error| error.get_ref())
                .map(|error| error as &(dyn std::error::Error + 'static))
                .or_else(|| error.source());
        }
        assert!(found,"original native error lost: {error:?}");
    }).await.unwrap();
}
