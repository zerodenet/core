//! Profiles that require native TLS/WS keep those real options and native owners.
use super::*;
#[tokio::test]
async fn custom_browser_profiles_preserve_native_tls_ws_and_retire_the_original_provider() {
    let _serial = SERIAL
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    tokio::time::timeout(Duration::from_secs(60), async {
        for kind in ["vmess", "trojan"] {
            let certificate = rcgen::generate_simple_self_signed(vec!["front.example".into()]).unwrap();
            let directory = tempfile::tempdir().unwrap();
            let cert = directory.path().join("cert.pem"); let key = directory.path().join("key.pem");
            std::fs::write(&cert, certificate.cert.pem()).unwrap(); std::fs::write(&key, certificate.signing_key.serialize_pem()).unwrap();
            let mut protocol = serde_json::json!({"type":kind,"ws":{"path":"/custom"},"tls":{"cert_path":cert,"key_path":key}});
            if kind == "trojan" { protocol["password"] = serde_json::json!(ID); }
            else { protocol["users"] = serde_json::json!([{"id":ID}]); }
            let remote = fixture::start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],"route":{"final":{"type":"direct"}}}),"remote").await;
            let mut outbound = serde_json::json!({"type":kind,"server":"127.0.0.1","port":remote.address.port(),"ws":{"path":"/custom","host":"front.example","headers":{"X-Zero-Profile":"retained"},"browser_dialer":{"listen":BROWSER,"idle_capacity":1}}});
            if kind == "trojan" { outbound["password"] = serde_json::json!(ID);outbound["sni"]=serde_json::json!("front.example");outbound["insecure"]=serde_json::json!(true); }
            else { outbound["id"]=serde_json::json!(ID);outbound["tls"]=serde_json::json!({"server_name":"front.example","ca_cert_path":cert}); }
            let client = fixture::start_owned(serde_json::json!({"runtime":{"native_stream_max_originals":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"node","protocol":outbound}],"route":{"final":{"type":"route","outbound":"node"}}}),"entry").await;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();let port=listener.local_addr().unwrap().port();
            let echo = tokio::spawn(async move { let (mut io, _) = listener.accept().await.unwrap(); let mut bytes=[0;17];io.read_exact(&mut bytes).await.unwrap();io.write_all(&bytes).await.unwrap();let mut eof=[0];assert_eq!(io.read(&mut eof).await.unwrap(),0);io.shutdown().await.unwrap(); });
            // There is no browser agent: the same explicitly selected native
            // profile must actually complete TLS/WS and protocol forwarding.
            let mut io = super::mux::connect(client.address, port).await;
            io.write_all(b"retained-profiles").await.unwrap();let mut got=[0;17];io.read_exact(&mut got).await.unwrap();assert_eq!(&got,b"retained-profiles");
            let plan=client.proxy.tcp_runtime_services().components.clone().unwrap();let node=plan.configured("node").unwrap();
            // The real CA File declaration can precede the native Stream.
            let ordinal=node.recipe.spec().resources.iter().position(|resource|matches!(resource,
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledNodeCarrier{..}
            )).expect("selected native TLS/WS carrier declaration");
            let owner=node.original_carrier_owner(ordinal).unwrap();let observed=owner.observe().unwrap();assert_eq!(observed.retained(),1);
            let original=observed.slots()[0].original().clone();assert!(observed.slots()[0].native().is_some());
            io.shutdown().await.unwrap();drop(io);echo.await.unwrap();let roots=client.proxy.protocols.runtime_host().original_budgets();client.close().await;
            assert!(owner.closed().unwrap().matches(&owner));assert!(original.matches_closed(&original.closed().unwrap()));assert_eq!(roots.carrier.snapshot().charged,0);
            remote.close().await;
        }
    }).await.unwrap();
}
