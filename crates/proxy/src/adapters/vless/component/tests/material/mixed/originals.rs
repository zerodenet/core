//! Exact installed registrations and actual consumed joins; no native guessing.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::{
    contract::{component::ResourceDeclaration as R, scope::TaskJoinWitness},
    controlled_carrier::{CarrierOriginObservation, CarrierOwnerRegistration},
    controlled_datagram::metadata_pool::{MetadataPoolOrigin, MetadataPoolRegistration},
};
pub(super) struct Originals {
    carriers: Vec<(CarrierOwnerRegistration, Vec<CarrierOriginObservation>)>,
    metadata: Vec<(MetadataPoolRegistration, Option<MetadataPoolOrigin>)>,
    drivers: Vec<TaskJoinWitness>,
    pub actual_halves: usize,
}
impl Originals {
    pub async fn capture(client: &Running, borrowed: bool) -> Self {
        let plan = client
            .proxy
            .tcp_runtime_services()
            .components
            .clone()
            .unwrap();
        let slot = plan.configured("node").unwrap();
        assert!(plan.has_cached_stream_provider(slot.role_id()));
        assert!(plan.stream_dependencies_supported(slot.role_id()));
        let mut actual = Self {
            carriers: vec![],
            metadata: vec![],
            drivers: vec![],
            actual_halves: 0,
        };
        for (ordinal, resource) in slot.recipe.spec().resources.iter().enumerate() {
            match resource {
                R::ControlledCarrierPool {
                    datagram,
                    max_sockets,
                    ..
                } => {
                    assert_eq!(max_sockets.get(), ORIGINALS);
                    let owner = slot.original_carrier_owner(ordinal).unwrap();
                    assert_eq!(owner.instance(), slot.instance_id());
                    let observed = owner.observe().unwrap();
                    assert!(observed.retained() <= ORIGINALS);
                    let mut originals = vec![];
                    for original in observed.slots() {
                        let auxiliary = original.auxiliary().expect("actual setup Aux");
                        let joined = auxiliary.wait_registration_released().await;
                        assert!(
                            auxiliary.matches_joined(&joined)
                                && joined.result().is_ok()
                                && joined.registration_released()
                        );
                        let driver = &original
                            .driver()
                            .expect("actual installed physical driver")
                            .1;
                        assert!(slot.matches_live_scope(&driver.scope_identity()));
                        assert!(driver.native_task_id().is_some());
                        if borrowed {
                            assert!(original.borrowed_origin().is_some());
                            assert!(original.native().is_none());
                        } else if !datagram {
                            assert!(original.native().is_some());
                        }
                        actual.drivers.push(driver.clone());
                        originals.push(original.original().clone());
                    }
                    if (!datagram || borrowed) && !originals.is_empty() {
                        actual.actual_halves += 1;
                    }
                    actual.carriers.push((owner, originals));
                }
                R::ControlledMetadataCarrier { max_origins, .. } => {
                    assert_eq!(max_origins.get(), ORIGINALS);
                    let owner = slot.original_metadata_owner(ordinal).unwrap();
                    assert_eq!(owner.instance(), slot.instance_id());
                    let origin = owner.current_original();
                    if let Some(origin) = origin.as_ref().filter(|_| !borrowed) {
                        let auxiliary = origin.initializer_registration().unwrap();
                        let joined = auxiliary.wait_registration_released().await;
                        assert!(
                            auxiliary.matches_joined(&joined)
                                && joined.result().is_ok()
                                && joined.registration_released()
                        );
                        let retained = origin
                            .retained_driver()
                            .expect("exact native queue and actual sealed driver ledger");
                        assert!(
                            slot.matches_live_scope(&retained.protocol_driver().scope_identity())
                        );
                        assert!(!retained.protocol_driver().is_joined());
                        actual.drivers.push(retained.protocol_driver().clone());
                        actual.actual_halves += 1;
                    }
                    actual.metadata.push((owner, origin));
                }
                R::File { .. } => {}
                _ => panic!("unexpected ordinary mixed declaration"),
            }
        }
        actual
    }
    pub fn same(&self, other: &Self) -> bool {
        self.carriers
            .iter()
            .flat_map(|(_, originals)| originals)
            .all(|original| {
                other
                    .carriers
                    .iter()
                    .flat_map(|(_, originals)| originals)
                    .any(|same| same.same_original(original))
            })
            && self
                .metadata
                .iter()
                .filter_map(|(_, original)| original.as_ref())
                .all(|original| {
                    other
                        .metadata
                        .iter()
                        .filter_map(|(_, original)| original.as_ref())
                        .any(|same| same.same_original(original))
                })
    }
    pub async fn closed(&self) {
        timeout(Duration::from_secs(3), async {
            for driver in &self.drivers {
                let joined = driver.wait_registration_released().await;
                assert!(
                    driver.matches_joined(&joined)
                        && joined.result().is_ok()
                        && joined.registration_released()
                );
            }
            for (owner, originals) in &self.carriers {
                assert!(owner.closed().unwrap().matches(owner));
                assert!(owner.users_closed().unwrap().matches(owner));
                for original in originals {
                    let proof = original
                        .closed()
                        .expect("matching actual original disposition");
                    assert!(original.matches_closed(&proof));
                }
            }
            for (owner, original) in &self.metadata {
                assert!(owner.closed().unwrap().matches(owner));
                if let Some(original) = original {
                    assert!(original.closed().unwrap().matches(original));
                    let observed = original.registration_observation();
                    assert!(observed.released().unwrap().matches(&observed));
                }
            }
        })
        .await
        .unwrap();
    }
}
pub(super) async fn udp(client: &Running) {
    timeout(Duration::from_secs(8), async {
        let (control, socket, relay) = super::super::udp::associate(client.address).await;
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = peer.local_addr().unwrap();
        let codec = socks5::udp::Socks5InboundUdpCodec;
        for payload in [
            b"".as_slice(),
            b"two real physical HTTP halves".as_slice(),
            b"".as_slice(),
        ] {
            let wire = codec
                .encode_response_to_client(
                    &zero_core::Address::Ipv4([127, 0, 0, 1]),
                    target.port(),
                    payload,
                )
                .unwrap();
            socket.send_to(&wire, relay).await.unwrap();
            let mut bytes = [0u8; 65536];
            let (size, from) = peer.recv_from(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], payload);
            peer.send_to(payload, from).await.unwrap();
            let (size, _) = socket.recv_from(&mut bytes).await.unwrap();
            let (address, port, body) = codec.decode_response(&bytes[..size]).unwrap().into_parts();
            assert_eq!(address, zero_core::Address::Ipv4([127, 0, 0, 1]));
            assert_eq!(port, target.port());
            assert_eq!(body.as_slice(), payload);
        }
        drop(socket);
        drop(control);
    })
    .await
    .unwrap();
}
