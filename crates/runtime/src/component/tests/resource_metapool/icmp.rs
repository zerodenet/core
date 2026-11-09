//! Real Host provider: actual UDP candidate, ICMP original and held caller ACK.
use super::*;
use zero_platform_tokio::{
    contract::{component::GrantedMetadataListener, scope::TaskClass},
    controlled_datagram::metadata_pool::{MetadataIcmpMemory, MetadataPoolRegistrationObservation},
    controlled_icmp::{ControlledIcmpSocket, IcmpAttemptDisposition, IcmpAttemptObservation},
};
struct Opened {
    permit: MetadataPoolPermit,
    wire: GrantedMetadataListener,
    icmp: ControlledIcmpSocket,
    observed: IcmpAttemptObservation,
    registration: MetadataPoolRegistrationObservation,
}
#[tokio::test]
#[ignore = "requires actual native RAW or permitted ICMP datagram provider"]
async fn actual_host_metadata_icmp_native_closes_before_its_exact_metadata_host_ack_returns_root_credit(
) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let end = Instant::now() + std::time::Duration::from_secs(3);
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let fixture = Fixture::with_icmp(remote).await;
        let originals = fixture.slot.bound_original_budgets().unwrap();
        let resources = fixture.resources.clone();
        let actual = fixture.slot.instance_id();
        let (begin, started) =
            tokio::sync::oneshot::channel::<(MetadataPoolPermit, TaskJoinWitness)>();
        let (ready, opened) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let (_, query) = fixture
            .tasks
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let (permit, query) = started.await.unwrap();
                let memory = MetadataIcmpMemory::reserve(&resources.packet_driver_metadata()?)?;
                let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
                let binding = zero_platform_tokio::contract::network::UpstreamBinding {
                    prefix: vec![],
                    dial: Default::default(),
                    network_generation: NetworkGeneration(egress.generation()),
                };
                let endpoint = zero_core::request::Destination::new(
                    zero_core::Address::Ipv4([127, 0, 0, 1]),
                    remote.port(),
                );
                use zero_platform_tokio::contract::network::UpstreamOpener;
                let candidates = zero_platform_tokio::controlled_carrier::NativeCarrierOpener::new(
                    actual,
                    binding.clone(),
                    remote,
                    egress,
                    None,
                )
                .unwrap()
                .resolve_controlled_carrier(
                    &binding,
                    endpoint,
                    zero_platform_tokio::contract::network::CarrierContext {
                        session: None,
                        deadline: Some(end),
                        cancel: Default::default(),
                    },
                )
                .await?;
                let (_, wire) = candidates.open_metadata(0, None, &permit, &query, end)?;
                let mut writable = wire.tx.create_write_poller().unwrap();
                let frame = MetadataTransmit {
                    destination: remote,
                    contents: b"actual metadata before ICMP",
                    source_ip: None,
                    ecn: None,
                    segment_size: None,
                };
                std::future::poll_fn(|cx| loop {
                    std::task::ready!(writable.poll_writable(cx)).unwrap();
                    match wire.tx.try_send(&frame) {
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
                        result => return std::task::Poll::Ready(result.unwrap()),
                    }
                })
                .await;
                drop(writable);
                let attempt = candidates.prepare_icmp(0, None, &permit, &query, memory)?;
                let observed = attempt.observation();
                let registration = permit.origin().registration_observation();
                let icmp = resources.bind_outbound_icmp(attempt)?;
                ready
                    .send(Opened {
                        permit,
                        wire,
                        icmp,
                        observed,
                        registration,
                    })
                    .unwrap_or_else(|_| panic!("actual original receiver remains live"));
                let _ = finished.await;
                Ok(())
            })
            .unwrap();
        let permit = fixture.grants.reserve(&query).unwrap();
        begin
            .send((permit, query.clone()))
            .unwrap_or_else(|_| panic!("actual initializer remains live"));
        let mut bytes = [0; 64];
        let (read, _) = fixture.drive(peer.recv_from(&mut bytes)).await.unwrap();
        assert_eq!(&bytes[..read], b"actual metadata before ICMP");
        let Opened {
            permit,
            wire,
            mut icmp,
            observed,
            registration,
        } = fixture.drive(opened).await.unwrap_or_else(|error| {
            let joined = query.try_joined();
            let actual = joined.as_ref().map(|joined| joined.result());
            let mut causes = Vec::new();
            if let Some(Err(ref source)) = actual {
                let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(source.as_ref());
                while let Some(original) = cause {
                    causes.push(original.to_string());
                    cause = original.source();
                }
            }
            panic!("actual ICMP initializer failed before original handoff: {error}; original Scope result={actual:?}; original source chain={causes:?}");
        });
        assert!(matches!(
            observed.disposition(),
            IcmpAttemptDisposition::Pending
        ));
        echo(&mut icmp,end).await;
        assert!(!observed.view_returned());
        assert!(registration.released().is_none());
        assert_eq!(originals.carrier.snapshot().charged, 2);
        drop(icmp);
        let close = fixture.drive(observed.wait_original()).await;
        assert!(
            matches!(close, IcmpAttemptDisposition::Closed(ref receipt) if receipt.matches_attempt(&observed)),
            "only this actual matching native close is positive"
        );
        assert!(observed.view_returned());
        assert_eq!(
            originals.carrier.snapshot().charged,
            2,
            "native close does not release original caller/metadata ACK"
        );
        let returned = wire
            .return_unstarted()
            .unwrap_or_else(|_| panic!("same actual metadata views returned"));
        let users = returned.try_closed().unwrap();
        let native = permit.close(Some(&users), &fixture.tasks).unwrap();
        assert!(native.matches(&permit.origin()) && native.had_native_acquisition());
        assert!(registration.released().is_none());
        assert!(query.try_joined().is_none());
        assert_eq!(fixture.grants.retained(), Some(1));
        assert_eq!(originals.carrier.snapshot().charged, 2);
        fixture.complete_query(&query, finish).await;
        let receipt = registration
            .released()
            .expect("same original removed only after actual Host ACK");
        assert!(receipt.matches(&registration));
        fixture
            .drive(std::future::poll_fn(|_| {
                if originals.carrier.snapshot().charged == 0 {
                    std::task::Poll::Ready(())
                } else {
                    std::task::Poll::Pending
                }
            }))
            .await;
        assert!(
            matches!(observed.disposition(), IcmpAttemptDisposition::Closed(_)),
            "positive original receipt survives actual provider reaping"
        );
        assert!(fixture.finish().await.is_complete());
    })
    .await
    .unwrap();
}

#[path = "icmp/rejected.rs"]
mod rejected;

async fn echo(socket: &mut ControlledIcmpSocket, end: Instant) {
    let target = std::net::Ipv4Addr::LOCALHOST.into();
    socket.connect(target).await.unwrap();
    let mut message = *b"\x08\0\0\0\x51\x79\0\x01actual-host-icmp";
    let mut sum = 0u32;
    for pair in message.chunks(2) {
        sum += u32::from(u16::from_be_bytes([pair[0], *pair.get(1).unwrap_or(&0)]));
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16)
    }
    message[2..4].copy_from_slice(&(!(sum as u16)).to_be_bytes());
    assert_eq!(socket.send(&message).await.unwrap(), message.len());
    let identifier = socket.reply_identifier(0x5179).unwrap();
    tokio::time::timeout_at(end, async {
        let mut bytes = [0; 1500];
        loop {
            let (size, source) = socket.recv_from(&mut bytes).await.unwrap();
            if source == target
                && size == message.len()
                && bytes[0] == 0
                && bytes[4..6] == identifier.to_be_bytes()
                && bytes[6..size] == message[6..]
            {
                break;
            }
        }
    })
    .await
    .unwrap();
}
