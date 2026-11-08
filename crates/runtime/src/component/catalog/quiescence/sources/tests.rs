//! Real role/provider/Scope source custody before IO and after actual close.
use super::*;
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::{
    contract::{
        lifecycle::ResourceLifecycle,
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        CancellationToken,
    },
    controlled_carrier::NativeCarrierOpener,
    EgressInterfaceControl,
};
mod fixture;
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(3)
}
fn contains(mut source: &(dyn Error + 'static), expected: &Arc<ChannelError>) -> bool {
    loop {
        if std::ptr::eq(source, expected.as_ref() as &(dyn Error + 'static)) {
            return true;
        }
        let Some(next) = source.source() else {
            return false;
        };
        source = next;
    }
}
#[tokio::test]
async fn actual_declared_capsule_survives_tcp_journal_failure_and_original_scope_retirement() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let endpoint = zero_core::request::Destination::new(
            zero_core::Address::Ipv4([127, 0, 0, 1]),
            peer.port(),
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("actual-journal");
        let catalog = ComponentCatalog::default();
        let (plan, slot, issued) = fixture::install(&catalog, endpoint.clone(), path.clone());
        let root = catalog.original_budgets();
        root.carrier
            .install_after_retirement(std::num::NonZeroUsize::new(2).unwrap())
            .unwrap();
        catalog.drive_supervised(plan.start(end())).await.unwrap();
        catalog.admission.publish([slot.admission_grant()]);
        assert!(
            slot.stream(end()).await.is_ok(),
            "genuine actual Direct Stream role"
        );
        let mut grant = issued.grants.get().unwrap().reserve().unwrap();
        let adoption = grant.adoption();
        let view = grant.claim_transferred_view().unwrap();
        let actual_scope = issued.tasks.get().unwrap().scope_identity();
        let source = catalog
            .owned_carrier_declared_sources(slot.role_id(), &adoption)
            .unwrap();
        assert_eq!(source.instance(), slot.instance_id());
        assert_eq!(
            root.carrier.snapshot().charged,
            2,
            "two actual graph reservations, not FD count"
        );
        assert!(adoption.closed().is_none());
        let mut foreign_role = slot.role_id();
        foreign_role.generation = ResourceGeneration(99);
        assert!(catalog
            .owned_carrier_declared_sources(foreign_role, &adoption)
            .is_err());
        // A genuine explicit cold owner with copied scalar identity still
        // cannot stand in for this catalog's actual registered provider.
        let mut unrelated = zero_platform_tokio::controlled_carrier::CarrierOwner::new(
            slot.instance_id(),
            std::num::NonZeroUsize::new(1).unwrap(),
            &CancellationToken::new(),
        );
        unrelated.start(end()).await.unwrap();
        let unrelated_grant = unrelated.grants().reserve().unwrap();
        assert!(catalog
            .owned_carrier_declared_sources(slot.role_id(), &unrelated_grant.adoption())
            .is_err());
        drop(unrelated_grant);
        assert!(unrelated.shutdown(end()).await.unwrap().is_complete());
        let egress = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: zero_core::component::NetworkGeneration(egress.generation()),
        };
        let opener =
            NativeCarrierOpener::new(slot.instance_id(), binding.clone(), peer, egress, None)
                .unwrap();
        let native = opener
            .prepare_controlled_stream(
                &binding,
                endpoint,
                CarrierContext {
                    session: None,
                    deadline: Some(end()),
                    cancel: CancellationToken::new(),
                },
                grant,
            )
            .unwrap()
            .open(end())
            .await
            .unwrap();
        let mut loan = native.into_parts().0.into_stream_loan(view).unwrap();
        let (mut remote, _) = listener.accept().await.unwrap();
        loan.io.write_all(b"actual").await.unwrap();
        let mut bytes = [0; 6];
        remote.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"actual");
        assert!(adoption.closed().is_none());
        std::fs::create_dir(&path).unwrap();
        assert!(issued
            .journal
            .get()
            .unwrap()
            .replace(1, |write| std::io::Write::write_all(write, b"x"))
            .is_err());
        // Capture the actual Root row before its local receiver can accept it
        // and allow an already-closed original to be reclaimed.
        let original = root
            .carrier
            .resource_failures()
            .into_iter()
            .find(|fault| fault.instance == slot.instance_id())
            .unwrap();
        let observed = slot.observe_resource_fault().unwrap();
        assert!(
            contains(&source, &original.source),
            "pre-IO capsule observes actual late OS failure"
        );
        assert!(!slot.matches_live_scope(&actual_scope));
        let cancelled = catalog
            .owned_carrier_declared_sources(slot.role_id(), &adoption)
            .unwrap();
        assert!(
            contains(&cancelled, &original.source),
            "cancel does not revoke diagnostic ownership"
        );
        drop(loan.io);
        assert!(loan.owner.close(end()).await.unwrap().is_complete());
        assert!(adoption.closed().unwrap().matches(&adoption));
        assert!(adoption.closed().unwrap().had_native_acquisition());
        assert_eq!(remote.read(&mut [0; 1]).await.unwrap(), 0);
        let report = catalog
            .drive_supervised(slot.shutdown(end()))
            .await
            .unwrap();
        assert!(report.is_complete(), "actual report: {report:?}");
        assert!(issued.journal.get().unwrap().native_closed());
        assert_eq!(root.carrier.snapshot().charged, 0);
        let retired = catalog
            .owned_carrier_declared_sources(slot.role_id(), &adoption)
            .unwrap();
        assert!(
            contains(&retired, &original.source),
            "same original Scope remains observable after drain"
        );
        let weak = Arc::downgrade(&original.source);
        drop(original);
        drop(observed);
        drop(report);
        drop(loan.owner);
        drop(adoption);
        drop(issued);
        drop(plan);
        drop(slot);
        drop(catalog);
        drop(root);
        assert!(weak.upgrade().is_some());
        drop(source);
        drop(cancelled);
        drop(retired);
        assert!(
            weak.upgrade().is_none(),
            "pure cause capsule contains no Host/Scope/controller cycle"
        );
    })
    .await
    .unwrap();
}
