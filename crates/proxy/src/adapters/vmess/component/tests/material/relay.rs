//! The same three actual Node owners serve Business and borrowed Service views.
use super::fixture;
use std::time::Duration;
use zero_platform_tokio::{
    contract::scope::TaskJoinWitness,
    controlled_carrier::{CarrierOriginObservation, CarrierOwnerRegistration},
};
struct Original {
    owner: CarrierOwnerRegistration,
    original: CarrierOriginObservation,
    driver: TaskJoinWitness,
    native: bool,
}
fn capture(client: &fixture::Running) -> Vec<Original> {
    let services = client.proxy.tcp_runtime_services();
    let plan = services.components.as_ref().unwrap();
    let mut originals: Vec<Original> = Vec::new();
    for index in 0..3 {
        let slot = plan.configured(&format!("node-{index}")).unwrap();
        let mut active = Vec::new();
        for ordinal in 0..slot.recipe.spec().resources.len() {
            let Some(owner) = slot.original_carrier_owner(ordinal) else {
                continue;
            };
            let observed = owner.observe().unwrap();
            assert_eq!(owner.limit().get(), 1);
            if observed.retained() == 0 {
                continue;
            }
            assert_eq!(observed.retained(), 1);
            let record = &observed.slots()[0];
            assert!(record.physical_view_live());
            assert!(record.acquisition_unknown().is_none());
            let (_, driver) = record
                .driver()
                .expect("same actual per-connection H2 driver");
            assert!(!driver.is_joined());
            let setup = record.auxiliary().expect("same sequential setup Auxiliary");
            let ready = setup
                .try_joined()
                .expect("preparation joined before driver delivery");
            assert!(setup.matches_joined(&ready) && ready.registration_released());
            assert!(setup.scope_identity().same_scope(&driver.scope_identity()));
            assert!(driver
                .scope_identity()
                .same_scope(&owner.bound().unwrap().scope_identity()));
            let native = record.native().is_some();
            assert_eq!(native, index == 0);
            if index == 0 {
                assert!(record.borrowed_origin().is_none());
            } else {
                let donor = record
                    .borrowed_origin()
                    .expect("real previous-role loan strongly retained");
                assert!(donor.owner().same_owner(&originals[index - 1].owner));
            }
            active.push(Original {
                owner: owner.clone(),
                original: record.original().clone(),
                driver: driver.clone(),
                native,
            });
        }
        assert_eq!(
            active.len(),
            1,
            "one current physical owner per configured node"
        );
        originals.push(active.pop().unwrap());
    }
    originals
}
async fn returned(originals: &[Original]) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if originals.iter().all(|original| {
                original.original.closed().is_some()
                    && (original
                        .owner
                        .observe()
                        .is_some_and(|observed| observed.retained() == 0)
                        || original
                            .owner
                            .closed()
                            .is_some_and(|closed| closed.matches(&original.owner)))
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for original in originals {
        let closed = original.original.closed().unwrap();
        assert!(original.original.matches_closed(&closed));
        assert_eq!(closed.had_native_acquisition(), original.native);
        let joined = original.driver.try_joined().unwrap();
        assert!(original.driver.matches_joined(&joined) && joined.registration_released());
    }
}
#[tokio::test]
async fn ordinary_three_grpc_nodes_keep_one_owner_each_for_tcp_halfclose_udp_and_empty_packets() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for kind in ["grpc-single", "grpc-multi"] {
            let directory = tempfile::tempdir().unwrap();
            let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let cert = directory.path().join("server.pem");
            let key = directory.path().join("server.key");
            std::fs::write(&cert, certificate.cert.pem()).unwrap();
            std::fs::write(&key, certificate.signing_key.serialize_pem()).unwrap();
            let remote = fixture::server(kind, &cert, &key).await;
            let nodes: Vec<_> = (0..3).map(|index| serde_json::json!({
                "tag":format!("node-{index}"),"protocol":{"type":"vmess","server":"127.0.0.1","port":remote.address.port(),
                "id":"11111111-2222-3333-4444-555555555555","tls":{"server_name":"localhost","ca_cert_path":cert,"client_fingerprint":null},
                "grpc":{"service_names":["original/Tun"],"multi_mode":kind=="grpc-multi"}}
            })).collect();
            let client = fixture::start(serde_json::json!({
                "runtime":{"vmess_grpc_max_originals":1},
                "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
                "outbounds":nodes,"outbound_groups":[{"tag":"chain","type":"relay","proxies":["node-0","node-1","node-2"]}],
                "route":{"final":{"type":"route","outbound":"chain"}}
            }), "entry").await;
            let first = fixture::echo_with(&client, || capture(&client)).await;
            returned(&first).await;
            let next = fixture::echo_with(&client, || capture(&client)).await;
            returned(&next).await;
            for (old, new) in first.iter().zip(&next) {
                assert!(old.owner.same_owner(&new.owner));
                assert!(!old.original.same_original(&new.original));
            }
            let udp = fixture::udp_echo_with(&client, || capture(&client)).await;
            client.close().await;
            returned(&udp).await;
            for original in first.iter().chain(&next).chain(&udp) {
                assert!(original.owner.closed().unwrap().matches(&original.owner));
                assert!(original.owner.users_closed().unwrap().matches(&original.owner));
            }
            remote.close().await;
        }
    }).await.expect("original three-hop wire and cleanup deadline");
}
