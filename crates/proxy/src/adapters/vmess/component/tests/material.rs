//! Ordinary full TLS/H2/upgrade callers retain their exact native originals.
mod fixture;
use fixture::*;
#[tokio::test]
async fn ordinary_raw_ws_and_grpc_keep_exact_material_native_users_and_ack_through_network_and_failed_reload(
) {
    tokio::time::timeout(std::time::Duration::from_secs(45), async {
        for kind in ["raw", "ws", "grpc-single", "grpc-multi"] {
            for backend in ["rustls", "openssl"] {
                let directory = tempfile::tempdir().unwrap();
                let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
                let cert_path = directory.path().join("server.pem");
                let key_path = directory.path().join("server.key");
                let ca_path = directory.path().join("client.pem");
                std::fs::write(&cert_path, cert.cert.pem()).unwrap();
                std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
                std::fs::write(&ca_path, cert.cert.pem()).unwrap();
                let remote = server(kind, &cert_path, &key_path).await;
                let client = client(kind, backend, remote.address, &ca_path).await;
                let (owner, first) = echo(&client).await;
                wait_returned(&owner, &first).await;
                std::fs::remove_file(&ca_path).unwrap();
                client.proxy.egress_interface.invalidate_network();
                let (same, second) = echo(&client).await;
                assert!(!first.same_original(&second));
                wait_returned(&same, &second).await;
                // New configuration candidates must not inherit by path equality.
                // A rejected parse restores the prior authorized immutable recipe.
                std::fs::write(&ca_path, b"this is not an authorized CA certificate").unwrap();
                let candidate = (*client.proxy.engine().config()).clone();
                assert!(
                    client
                        .handle()
                        .apply_runtime_config_and_wait(candidate, std::time::Duration::from_secs(5))
                        .await
                        .is_err(),
                    "fresh candidate must actually read and validate the changed CA"
                );
                let (restored, third) = echo(&client).await;
                wait_returned(&restored, &third).await;
                client.close().await;
                for owner in [owner, same, restored] {
                    assert!(owner.closed().unwrap().matches(&owner));
                    assert!(owner.users_closed().unwrap().matches(&owner));
                }
                remote.close().await;
            }
        }
    })
    .await
    .unwrap();
}
