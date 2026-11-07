//! Ordinary full TLS/H2/upgrade callers retain their exact native originals.
#[path = "material/domain.rs"]
mod domain;
#[path = "material/fixture.rs"]
mod fixture;
#[path = "material/mux.rs"]
mod mux;
#[path = "material/mux_material.rs"]
mod mux_material;
#[path = "material/read_progress.rs"]
mod read_progress;
#[path = "material/udp.rs"]
mod udp;
use fixture::*;
#[tokio::test]
async fn ordinary_h2_and_upgrade_use_the_same_owned_file_material_across_network_and_failed_reload()
{
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for kind in ["h2", "http_upgrade"] {
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
                let rejected = client
                    .handle()
                    .apply_runtime_config_and_wait(candidate, std::time::Duration::from_secs(5))
                    .await;
                if let Err(error) = &rejected {
                    eprintln!("actual bad-CA candidate mode={kind} backend={backend} complete apply/recovery error: {error}");
                }
                assert!(rejected.is_err(), "fresh candidate must actually read and validate the changed CA");
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

#[path = "material/direct.rs"]
mod direct;
