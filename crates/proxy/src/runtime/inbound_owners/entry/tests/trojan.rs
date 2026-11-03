//! Production standard source: consume the actual host and original native receipts.
use zero_config::RuntimeConfig;

#[tokio::test]
async fn standard_udp_source_closes_its_original_scopes_and_native_providers() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    use std::{sync::Arc, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream, UdpSocket},
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        let material = tempfile::tempdir().unwrap();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = material.path().join("cert.pem"); let key = material.path().join("key.pem");
        std::fs::write(&cert,certificate.cert.pem()).unwrap(); std::fs::write(&key,certificate.signing_key.serialize_pem()).unwrap();
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap(); let endpoint=reservation.local_addr().unwrap();
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap(); let address=target.local_addr().unwrap();
        let config = RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"trojan-in","listen":{"address":"127.0.0.1","port":endpoint.port()},"protocol":{"type":"trojan","password":"test-password","tls":{"cert_path":cert,"key_path":key}}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy=crate::Proxy::new(config).unwrap(); drop(reservation); let running=proxy.spawn();
        let socket=loop { match TcpStream::connect(endpoint).await { Ok(socket)=>break socket,Err(_)=>tokio::time::sleep(Duration::from_millis(5)).await } };
        let mut roots=rustls::RootCertStore::empty(); roots.add(certificate.cert.der().clone()).unwrap();
        let connector=tokio_rustls::TlsConnector::from(Arc::new(rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth()));
        let mut client=connector.connect(rustls::pki_types::ServerName::try_from("localhost").unwrap().to_owned(),socket).await.unwrap();
        let original=proxy.inbound_owners.owner("trojan-in").unwrap();
        let mut request=b"b42338ef718bb33b88d3c2bf0cb6130499a1dc05e5c101b3722b09c7\r\n".to_vec();
        request.extend([3,1,127,0,0,1]);request.extend(address.port().to_be_bytes());request.extend(b"\r\n");
        request.extend([1,127,0,0,1]);request.extend(address.port().to_be_bytes());request.extend([0,1,b'\r',b'\n',b'd']);
        client.write_all(&request).await.unwrap();
        let mut bytes=[0;16]; let (read, peer)=target.recv_from(&mut bytes).await.unwrap();assert_eq!(&bytes[..read],b"d");
        target.send_to(b"d",peer).await.unwrap();let mut response=[0;12];client.read_exact(&mut response).await.unwrap();assert_eq!(response[11],b'd');
        assert!(original.terminal().is_none());
        running.shutdown().await.unwrap();
        let terminal=original.terminal().expect("same original owner, not a rebound port or task body marker");
        assert!(terminal.failure().is_none());assert!(terminal.task_failures().is_empty());
        assert_eq!(terminal.joined_tasks(),3,"source, real handler and actual UDP driver Scope joins");
        assert_eq!(terminal.closed_io_receipts(),3,"original TCP listener, accepted carrier and peer Direct UDP native close receipts");
    }).await.unwrap();
}
