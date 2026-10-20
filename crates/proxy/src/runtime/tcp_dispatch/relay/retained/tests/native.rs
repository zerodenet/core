//! Stream-only native-prefix coverage. The production legacy SOCKS Datagram
//! recipe remains unsupported by retained admission and is not borrowed here.
use super::*;
use crate::component::direct_bridge::ServiceInvocation;
use zero_core::request::ServicePurpose;
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(5)
}
fn proxy(port: u16) -> Proxy {
    let mut input = value(port);
    input["outbounds"][0]["protocol"] =
        serde_json::json!({"type":"socks5","server":"127.0.0.1","port":port});
    Proxy::new(config(&input)).unwrap()
}
fn prefix(proxy: &Proxy) -> PreparedTcpRelayPrefix {
    let (services, mut prefix) = prepare(proxy);
    let slot = services
        .components
        .as_ref()
        .unwrap()
        .configured("first")
        .unwrap();
    prefix.first.execution = PreparedTcpCandidateExecution::Native(slot);
    // A service Stream prefix deliberately has no Datagram grant. This does
    // not qualify or change the production legacy associated-UDP preparation.
    #[cfg(feature = "udp-runtime")]
    prefix.datagram_prefixes.clear();
    prefix
}
async fn start(proxy: &Proxy) {
    let snapshot = proxy.engine().runtime_snapshot();
    proxy
        .protocols
        .start_components(snapshot.config().clone(), deadline())
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(snapshot.config(), || Ok(()))
        .unwrap();
}
fn services(proxy: &Proxy) -> TcpExecutionServices {
    proxy
        .tcp_runtime_services()
        .with_service_invocation(ServiceInvocation {
            deadline: deadline(),
            purpose: ServicePurpose::DiagnosticProbe,
        })
        .execution()
}
#[tokio::test]
async fn native_retained_admission_requires_actual_host_grant_and_request_identity() {
    let proxy = proxy(34567);
    let prefix = prefix(&proxy);
    let call = services(&proxy);
    assert!(
        check_owners(&prefix).is_err(),
        "cold preparation cannot mint admission"
    );
    assert!(matches!(
        RetainedSource::capture(&call, &prefix),
        RetainedSource::Rejected(_)
    ));
    start(&proxy).await;
    check_owners(&prefix).unwrap();
    let no_identity = proxy.tcp_runtime_services().execution();
    assert!(
        matches!(
            RetainedSource::capture(&no_identity, &prefix),
            RetainedSource::Legacy(_)
        ),
        "no native retained authority is granted without actual request facts"
    );
    let retained = bound(&call, &prefix);
    retained.services(&prefix).unwrap();
    proxy
        .protocols
        .shutdown_components(deadline())
        .await
        .unwrap();
    assert!(
        retained.services(&prefix).is_err(),
        "retired actual owner cannot be revived by static equality"
    );
}

#[tokio::test]
async fn actual_native_socks_prefix_reconnects_with_service_facts_and_stops_on_owner_retirement() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = proxy(listener.local_addr().unwrap().port());
    start(&proxy).await;
    let prefix = prefix(&proxy);
    let call = services(&proxy);
    bound(&call, &prefix);
    let carrier = super::super::super::prepare_lazy_tcp_relay_prefix(call, prefix);
    let connector = carrier.connector().unwrap();
    drop(carrier);
    let peer = async {
        for payload in [7, 9] {
            let (mut io, _) = listener.accept().await.unwrap();
            let mut greeting = [0; 3];
            io.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting, [5, 1, 0]);
            io.write_all(&[5, 0]).await.unwrap();
            let mut header = [0; 4];
            io.read_exact(&mut header).await.unwrap();
            assert_eq!(header, [5, 1, 0, 3]);
            let len = io.read_u8().await.unwrap();
            let mut host = vec![0; len as usize];
            io.read_exact(&mut host).await.unwrap();
            assert_eq!(host, b"127.0.0.1");
            assert_eq!(io.read_u16().await.unwrap(), 45873);
            io.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
                .await
                .unwrap();
            assert_eq!(io.read_u8().await.unwrap(), payload);
            io.write_all(&[payload]).await.unwrap();
        }
    };
    let client = async {
        for payload in [7, 9] {
            let mut io = connector.connect("127.0.0.1".into(), 45873).await.unwrap();
            io.write_all(&[payload]).await.unwrap();
            io.flush().await.unwrap();
            assert_eq!(io.read_u8().await.unwrap(), payload);
        }
        proxy
            .protocols
            .shutdown_components(deadline())
            .await
            .unwrap();
        assert!(connector.connect("127.0.0.1".into(), 45873).await.is_err());
        assert!(proxy.engine().active_sessions().is_empty());
        assert!(proxy.engine().completed_sessions().is_empty());
    };
    tokio::time::timeout_at(deadline(), async {
        tokio::join!(peer, client);
    })
    .await
    .unwrap();
}
