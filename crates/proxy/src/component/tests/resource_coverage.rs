use super::*;
use std::{num::NonZeroUsize, time::Duration};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::component::{DeviceOwnership, ResourceAccess};
fn one() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn endpoint() -> Destination {
    Destination::new(Address::Ipv4([127, 0, 0, 1]), 0)
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[cfg(unix)]
#[tokio::test]
async fn process_only_coverage_stays_unaccounted_with_a_real_live_then_reaped_child() {
    let grants = HostResources::new(
        ComponentId(521),
        &[ResourceDeclaration::Process {
            program: "python3".into(),
            max_starts: one(),
        }],
        CancellationToken::new(),
    )
    .unwrap();
    let access = grants.activate(deadline()).unwrap();
    let mut request = zero_platform_tokio::contract::component::ProcessRequest::new("python3");
    request.arguments = vec!["-c".into(), "import time;time.sleep(60)".into()];
    let mut child = grants.spawn_process(request).unwrap();
    assert!(child.id().is_some());
    assert!(child.try_wait().unwrap().is_none());
    let proof = grants.io_evidence();
    assert_eq!(proof.declared_count, 1);
    assert_eq!(proof.declarations[0].kind, DeclaredResourceKind::Process);
    assert_eq!(
        proof.declarations[0].provider,
        DeclarationProvider::Unaccounted
    );
    assert!(proof.files.is_empty() && proof.native.is_empty());
    assert!(!proof.physical_confirmed());
    child.start_kill().unwrap();
    let actual_status = tokio::time::timeout_at(deadline(), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child.try_wait().unwrap(), Some(actual_status));
    drop(access);
    assert!(grants.shutdown_io(deadline()).await.unwrap().is_complete());
    let proof = grants.io_evidence();
    assert!(proof.admission_closed);
    assert!(
        !proof.physical_confirmed(),
        "bare Child reap has no matching provider witness yet"
    );
}

#[tokio::test]
async fn canonical_file_tcp_udp_entries_follow_original_ordinals_and_real_closure() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "zero-coverage-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, b"four").unwrap();
    struct FileCleanup(PathBuf);
    impl Drop for FileCleanup {
        fn drop(&mut self) {
            std::fs::remove_file(&self.0).unwrap();
        }
    }
    let _file = FileCleanup(path.clone());
    let declarations = [
        ResourceDeclaration::DatagramListener {
            destination: endpoint(),
            max_bind_attempts: one(),
            max_pending_sends: one(),
        },
        ResourceDeclaration::File {
            path: path.clone(),
            max_bytes: 4,
        },
        ResourceDeclaration::TcpListener {
            destination: endpoint(),
            max_bind_attempts: one(),
        },
    ];
    let grants =
        HostResources::new(ComponentId(522), &declarations, CancellationToken::new()).unwrap();
    let cold = grants.io_evidence();
    assert_eq!(cold.declared_count, 3);
    assert_eq!(
        cold.declarations
            .iter()
            .map(|entry| (entry.ordinal, entry.kind))
            .collect::<Vec<_>>(),
        vec![
            (0, DeclaredResourceKind::DatagramListener),
            (1, DeclaredResourceKind::File),
            (2, DeclaredResourceKind::TcpListener),
        ]
    );
    assert!(cold.native.iter().all(|owner| owner.known_no_acquisition));
    assert!(matches!(
        cold.files[0].read.native,
        zero_platform_tokio::file::FileNativeEvidence::KnownNoAcquisition
    ));
    assert!(cold.physical_confirmed());
    assert!(
        !cold.admission_closed,
        "no-acquisition observation cannot seal open admission"
    );
    let _access = grants.activate(deadline()).unwrap();
    let tcp = grants.listen_tcp(endpoint()).await.unwrap();
    let address = tcp.local_addr().unwrap();
    let udp = grants.listen_datagram(endpoint()).await.unwrap();
    assert_eq!(grants.read_file(path, 4).await.unwrap(), b"four");
    let live = grants.io_evidence();
    assert!(live.native.iter().all(|owner| !owner.known_no_acquisition));
    assert!(!live.physical_confirmed());
    let report = grants.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 3);
    assert_eq!(report.released_resources.len(), 1);
    let closed = grants.io_evidence();
    assert!(closed.admission_closed && closed.physical_confirmed());
    assert!(tcp.local_addr().is_err());
    assert!(udp
        .tx
        .send_to_addr(b"closed", "127.0.0.1:9".parse().unwrap())
        .await
        .is_err());
    std::net::TcpListener::bind(address).unwrap();
    let mut missing = closed.clone();
    missing.declarations.clear();
    assert!(
        !missing.physical_confirmed(),
        "filtered empty subset cannot prove full coverage"
    );
    let mut duplicate = closed.clone();
    duplicate.declarations[2].provider = duplicate.declarations[0].provider;
    assert!(
        !duplicate.physical_confirmed(),
        "two declarations cannot share one native witness"
    );
    let mut wrong_ordinal = closed;
    wrong_ordinal.declarations[0].ordinal = 2;
    assert!(!wrong_ordinal.physical_confirmed());
}

#[test]
fn missing_providers_are_explicitly_unaccounted_without_authorizing_their_construction() {
    use zero_core::component::NetworkGeneration;
    let declarations = [
        (
            ResourceDeclaration::Device {
                name: "owned".into(),
                ownership: DeviceOwnership::Owned,
            },
            DeclaredResourceKind::Device,
        ),
        (
            ResourceDeclaration::Device {
                name: "borrowed".into(),
                ownership: DeviceOwnership::Borrowed,
            },
            DeclaredResourceKind::Device,
        ),
        (
            ResourceDeclaration::BackgroundCarrier,
            DeclaredResourceKind::BackgroundCarrier,
        ),
        (
            ResourceDeclaration::OutboundDatagram {
                peer: "127.0.0.1:9".parse().unwrap(),
                network_generation: NetworkGeneration(1),
                max_sockets: one(),
            },
            DeclaredResourceKind::OutboundDatagram,
        ),
        (
            ResourceDeclaration::BoundOutboundDatagram {
                max_sockets: one(),
                max_bindings: one(),
            },
            DeclaredResourceKind::BoundOutboundDatagram,
        ),
    ];
    for (declaration, kind) in declarations {
        let binding = CoverageBinding::new(ComponentId(523), &declaration).unwrap();
        let evidence = binding.observe(0, &[], &[]);
        assert_eq!(evidence.kind, kind);
        assert_eq!(evidence.provider, DeclarationProvider::Unaccounted);
        assert!(
            HostResources::new(ComponentId(523), &[declaration], CancellationToken::new()).is_err()
        );
    }
    let empty = HostResources::new(ComponentId(524), &[], CancellationToken::new())
        .unwrap()
        .io_evidence();
    assert_eq!(empty.declared_count, 0);
    assert!(empty.declarations.is_empty());
    assert!(
        empty.physical_confirmed(),
        "only the actual empty canonical resource set is vacuous"
    );
}
