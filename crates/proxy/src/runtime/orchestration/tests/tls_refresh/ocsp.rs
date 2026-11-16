//! Actual AIA/redirect requests and real native pending peer -> Source stop.
use super::*;
use std::{future::Future, task::Poll};
fn field(tag: u8, data: &[u8]) -> Vec<u8> {
    assert!(data.len() < 128);
    [&[tag, data.len() as u8], data].concat()
}
fn material(url: &str) -> (rcgen::Certificate, rcgen::KeyPair, rcgen::Certificate) {
    let issuer_key = rcgen::KeyPair::generate().unwrap();
    let mut issuer = rcgen::CertificateParams::new(vec![]).unwrap();
    issuer.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = issuer.self_signed(&issuer_key).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let mut leaf = rcgen::CertificateParams::new(vec!["ocsp.test".into()]).unwrap();
    let mut access = Vec::new();
    for (kind, url) in [(1, format!("{url}/status")), (2, format!("{url}/issuer"))] {
        let mut description = vec![6, 8, 0x2b, 6, 1, 5, 5, 7, 0x30, kind];
        description.extend(field(0x86, url.as_bytes()));
        access.extend(field(0x30, &description));
    }
    leaf.custom_extensions
        .push(rcgen::CustomExtension::from_oid_content(
            &[1, 3, 6, 1, 5, 5, 7, 1, 1],
            field(0x30, &access),
        ));
    (
        leaf.signed_by(&key, &rcgen::Issuer::from_params(&issuer, &issuer_key))
            .unwrap(),
        key,
        ca,
    )
}
const STAPLE: &[u8] = &[0x30, 0x03, 0x0a, 0x01, 0x06]; // DER OCSP unauthorized; same opaque staple policy as the library.
async fn reply(io: &mut tokio::net::TcpStream, body: &[u8]) {
    io.write_all(
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    io.write_all(body).await.unwrap();
    io.shutdown().await.unwrap();
}
#[tokio::test]
async fn actual_ocsp_originals_follow_issuer_redirect_staple_and_source_stop() {
    timeout(Duration::from_secs(40),async {
        for backend in ["rustls","openssl"] {
            let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let (leaf,key,issuer)=material(&format!("http://{}",listener.local_addr().unwrap()));
            let (replacement,replacement_key,replacement_issuer)=material("http://127.0.0.1:1");
            let directory=tempfile::tempdir().unwrap();let cert=directory.path().join("cert.pem");let private=directory.path().join("key.pem");
            // Missing issuer is fetched through the exact same native provider.
            std::fs::write(&cert,leaf.pem()).unwrap();std::fs::write(&private,key.serialize_pem()).unwrap();
            let issuer_der=issuer.der().to_vec();let (pending_tx,pending_rx)=tokio::sync::oneshot::channel();
            let server=tokio::spawn(async move {
                let mut pending_tx=Some(pending_tx);
                for request in 0..5 {
                    let (mut io,_)=listener.accept().await.unwrap();let headers=fixture::headers(&mut io).await;
                    match request {
                        0|3=>{assert!(headers.starts_with("GET /issuer "));reply(&mut io,&issuer_der).await;},
                        1=>{assert!(headers.starts_with("POST /status "));io.write_all(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();io.shutdown().await.unwrap();},
                        2=>{assert!(headers.starts_with("POST /final "));reply(&mut io,STAPLE).await;},
                        4=>{
                            assert!(headers.starts_with("POST /status "));
                            let mut byte=[0];let mut read=std::pin::pin!(io.read(&mut byte));
                            std::future::poll_fn(|cx|{assert!(read.as_mut().poll(cx).is_pending(),"actual original peer remains open with its response withheld");Poll::Ready(())}).await;
                            pending_tx.take().unwrap().send(()).unwrap();assert_eq!(read.await.unwrap(),0,"that original provider closed; no replacement port probe");
                        },_=>unreachable!(),
                    }
                }
            });
            let (proxy,running,endpoint)=fixture::start(json!({"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":"11111111-2222-3333-4444-555555555555"}],"tls":{"cert_path":cert.clone(),"key_path":private.clone(),"options":{"backend":backend,"reload_interval_secs":1,"ocsp_stapling_secs":1}}}}],"route":{"final":{"type":"direct"}}})).await;
            let original=owner(&proxy,"entry").await;let scope=original.original_scope_for_test();let (config,staple)=fixture::client(vec![issuer.der().clone(),replacement_issuer.der().clone()]);
            timeout(Duration::from_secs(5),async{loop{fixture::peek(endpoint,&config,"ocsp.test").await;if staple.lock().unwrap().as_slice()==STAPLE{break;}tokio::task::yield_now().await;}}).await.unwrap();
            pending_rx.await.unwrap();
            std::fs::write(&cert,replacement.pem()).unwrap();std::fs::write(&private,replacement_key.serialize_pem()).unwrap();
            timeout(Duration::from_secs(5),async{loop{if fixture::peek(endpoint,&config,"ocsp.test").await==replacement.der().as_ref(){break;}tokio::time::sleep(Duration::from_millis(20)).await;}}).await.expect("material rotates while the real OCSP response is still withheld");
            running.shutdown().await.unwrap();server.await.unwrap();
            assert!(!scope.resource_open());closed(&original);
            assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0,"original listener/HTTP/user/driver debts are retired");
        }
    }).await.unwrap();
}
