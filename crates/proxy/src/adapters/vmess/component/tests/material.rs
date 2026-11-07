//! Ordinary full TLS/H2/upgrade callers retain their exact native originals.
#[path = "material/fixture.rs"]
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

#[tokio::test]
async fn ordinary_mux_full_tls_transports_use_same_protocol_file_material_and_cached_driver_through_network_and_failed_reload(
) {
    tokio::time::timeout(std::time::Duration::from_secs(45), async {
        for kind in ["mux-raw", "mux-ws", "mux-grpc-single", "mux-grpc-multi"] {
            for backend in ["rustls", "openssl"] {
                let directory = tempfile::tempdir().unwrap();
                let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
                let certificate = directory.path().join("server.pem");
                let key = directory.path().join("server.key");
                let ca = directory.path().join("client.pem");
                std::fs::write(&certificate, cert.cert.pem()).unwrap();
                std::fs::write(&key, cert.signing_key.serialize_pem()).unwrap();
                std::fs::write(&ca, cert.cert.pem()).unwrap();
                let remote = server(kind, &certificate, &key).await;
                let client = client(kind, backend, remote.address, &ca).await;
                let (owner, original) = echo(&client).await;
                let first_driver = cached_driver(&owner, &original);
                udp_echo(&client).await;
                let (same, reused) = echo(&client).await;
                assert!(owner.same_owner(&same) && original.same_original(&reused));
                assert!(original.closed().is_none());
                assert_eq!(owner.observe().unwrap().retained(), 1);
                std::fs::remove_file(&ca).unwrap();
                client.proxy.egress_interface.invalidate_network();
                let (next_owner, next) = echo(&client).await;
                let next_driver = cached_driver(&next_owner, &next);
                assert!(!original.same_original(&next));
                assert!(original.matches_closed(&original.closed().unwrap()));
                std::fs::write(&ca, b"not an authorized CA").unwrap();
                let candidate = (*client.proxy.engine().config()).clone();
                assert!(client
                    .handle()
                    .apply_runtime_config_and_wait(candidate, std::time::Duration::from_secs(5))
                    .await
                    .is_err());
                let (restored, fresh) = echo(&client).await;
                let fresh_driver = cached_driver(&restored, &fresh);
                client.close().await;
                for driver in [first_driver, next_driver, fresh_driver] {
                    let joined = driver.try_joined().unwrap();
                    assert!(driver.matches_joined(&joined) && joined.registration_released());
                }
                for (owner, original) in [(owner, original), (next_owner, next), (restored, fresh)]
                {
                    let closed = original.closed().unwrap();
                    assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
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

#[path = "material/relay.rs"]
mod relay;

#[path = "material/domain.rs"]
mod domain;
