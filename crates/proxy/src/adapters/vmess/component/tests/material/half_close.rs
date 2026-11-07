//! Ordinary VMess+TLS preserves full reverse data after its framed upload EOF.
use super::fixture::*;
#[tokio::test]
async fn ordinary_vmess_tls_upload_halfclose_returns_all_256k_and_1025_tail_with_matching_originals(
) {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        for backend in ["rustls", "openssl"] {
            let directory = tempfile::tempdir().unwrap();
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let certificate = directory.path().join("server.pem");
            let key = directory.path().join("server.key");
            std::fs::write(&certificate, cert.cert.pem()).unwrap();
            std::fs::write(&key, cert.signing_key.serialize_pem()).unwrap();
            let remote = server("raw", &certificate, &key).await;
            let client = client("raw", backend, remote.address, &certificate).await;
            let (owner, original) =
                echo_response_with(&client, || capture(&client.proxy), 1025).await;
            wait_returned(&owner, &original).await;
            client.close().await;
            assert!(original.matches_closed(&original.closed().unwrap()));
            assert!(owner.closed().unwrap().matches(&owner));
            assert!(owner.users_closed().unwrap().matches(&owner));
            remote.close().await;
        }
    })
    .await
    .expect("original 8s TCP/TLS full-payload and tail deadline");
}
