//! Real Proxy -> Browser -> VMess/Trojan MUX, with the original lower receipts.
use super::*;
pub(super) async fn connect(address: std::net::SocketAddr, destination: u16) -> TcpStream {
    let mut io = TcpStream::connect(address).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend(destination.to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[1], 0);
    io
}
#[tokio::test]
async fn browser_mux_reuses_one_physical_original_for_two_streams_and_consumes_exact_close_receipts(
) {
    let _serial = SERIAL
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    tokio::time::timeout(Duration::from_secs(60), async {
        for kind in ["vmess", "trojan"] {
            let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
            let directory = tempfile::tempdir().unwrap();
            let cert = directory.path().join("cert.pem"); let key = directory.path().join("key.pem");
            std::fs::write(&cert, certificate.cert.pem()).unwrap(); std::fs::write(&key, certificate.signing_key.serialize_pem()).unwrap();
            let mut protocol = serde_json::json!({"type":kind,"ws":{"path":"/original"}});
            if kind == "trojan" { protocol["password"] = serde_json::json!(ID); protocol["tls"] = serde_json::json!({"cert_path":cert,"key_path":key}); }
            else { protocol["users"] = serde_json::json!([{"id":ID}]); }
            let remote = fixture::start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],"route":{"final":{"type":"direct"}}}),"remote").await;
            let mut outbound = serde_json::json!({"type":kind,"server":"127.0.0.1","port":remote.address.port(),"mux_concurrency":4,"ws":{"path":"/original","browser_dialer":{"listen":BROWSER,"idle_capacity":1}}});
            outbound[if kind == "trojan" { "password" } else { "id" }] = serde_json::json!(ID);
            let client = fixture::start_owned(serde_json::json!({"runtime":{"native_stream_max_originals":1,"native_mux_max_carriers":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"node","protocol":outbound}],"route":{"final":{"type":"route","outbound":"node"}}}),"entry").await;
            let mut roots = rustls::RootCertStore::empty(); roots.add(certificate.cert.der().clone()).unwrap();
            let agent = tokio::spawn(relay_browser(browser().await, roots));
            // The actual listener remains bound until its original worker owns it.
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap(); let port = listener.local_addr().unwrap().port();
            let echo = tokio::spawn(async move {
                let mut workers = Vec::new();
                for _ in 0..2 { let (mut io, _) = listener.accept().await.unwrap(); workers.push(tokio::spawn(async move { let mut byte=[0];io.read_exact(&mut byte).await.unwrap();io.write_all(&byte).await.unwrap();let mut rest=[0];assert_eq!(io.read(&mut rest).await.unwrap(),0);io.shutdown().await.unwrap(); })); }
                for worker in workers { worker.await.unwrap(); }
            });
            let mut first = connect(client.address, port).await; let mut second = connect(client.address, port).await;
            for (io, byte) in [(&mut first, 0x39), (&mut second, 0x71)] { io.write_all(&[byte]).await.unwrap(); let mut got=[0];io.read_exact(&mut got).await.unwrap();assert_eq!(got,[byte]); }
            let plan = client.proxy.tcp_runtime_services().components.clone().unwrap();
            let node = plan.configured("node").unwrap(); assert!(node.original_carrier_owner(0).is_none());
            let pool = plan.component(node.dependency_instances()[0].component).unwrap();
            assert!(pool.recipe.spec().roles.is_empty(), "the actual shared MUX owner has no callable role");
            assert!(plan.role_slot(pool.role_id()).is_none(), "a fabricated role cannot name this original owner");
            let pool_owner = pool.original_carrier_owner(0).unwrap(); let physical = pool_owner.observe().unwrap();
            assert_eq!(physical.retained(), 1, "both business streams reuse the original physical MUX");
            let original = physical.slots()[0].original().clone(); let driver = physical.slots()[0].driver().unwrap().1.clone();
            assert!(physical.slots()[0].native().is_none(), "MUX borrows the actual Browser original");
            let raw = plan.component(pool.dependency_instances()[0].component).unwrap();
            let raw_owner = raw.original_carrier_owner(0).unwrap(); let raw_observed = raw_owner.observe().unwrap();
            assert_eq!(raw_observed.retained(), 1); let raw_original = raw_observed.slots()[0].original().clone();
            let source = raw_observed.slots()[0].borrowed_origin().unwrap().clone();
            let borrowed = physical.slots()[0].borrowed_origin().unwrap().clone();
            assert_eq!(borrowed.instance(), raw.instance_id());
            assert!(source.accepted().is_some()); assert!(source.closed().is_none());
            datagram_exchange(&client).await;
            assert_eq!(pool_owner.observe().unwrap().retained(), 1);
            first.shutdown().await.unwrap(); second.shutdown().await.unwrap(); drop(first); drop(second); echo.await.unwrap();
            let budgets = client.proxy.protocols.runtime_host().original_budgets(); client.close().await;
            agent.await.unwrap();
            assert!(original.matches_closed(&original.closed().unwrap()));
            let joined = driver.try_joined().unwrap(); assert!(driver.matches_joined(&joined));
            assert!(pool_owner.closed().unwrap().matches(&pool_owner)); assert!(raw_owner.closed().unwrap().matches(&raw_owner));
            assert!(raw_original.matches_closed(&raw_original.closed().unwrap()));
            assert!(raw_original.matches_closed(&borrowed.closed().unwrap()));
            assert!(source.accepted().unwrap().is_settled()); assert!(source.closed().unwrap().matches(&source));
            assert_eq!(budgets.carrier.snapshot().charged, 0);
            remote.close().await;
        }
    }).await.unwrap();
}
