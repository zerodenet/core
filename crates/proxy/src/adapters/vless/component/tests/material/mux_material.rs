//! Full material stays with the original protocol MUX owner, not a bare leaf.
use super::fixture::*;
#[tokio::test]
async fn ordinary_mux_raw_ws_h2_and_upgrade_keep_same_file_material_original_driver_and_ack() {
    tokio::time::timeout(std::time::Duration::from_secs(45), async {
        for kind in ["mux-raw", "mux-ws", "mux-h2", "mux-http_upgrade"] {
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
                eprintln!("actual MUX mode={kind} backend={backend} phase=first TCP");
                let (owner, original) = echo(&client).await;
                eprintln!("actual MUX mode={kind} backend={backend} phase=first TCP complete");
                let first_driver = cached_driver(&owner, &original);
                let (same, reused) = echo(&client).await;
                assert!(owner.same_owner(&same) && original.same_original(&reused));
                assert!(original.closed().is_none());
                assert_eq!(owner.observe().unwrap().retained(), 1);
                std::fs::remove_file(&ca).unwrap();
                eprintln!("actual MUX mode={kind} backend={backend} phase=network epoch");
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
                eprintln!("actual MUX mode={kind} backend={backend} phase=actual host shutdown");
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
