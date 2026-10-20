use super::*;
use crate::{
    contract::{Impact, Reason, Stage},
    EgressInterfaceControl,
};
use std::{error::Error, num::NonZeroUsize, sync::Arc};
use zero_core::component::{
    ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration,
};
use zero_traits::DialPolicy;

fn issuer() -> DatagramBindingIssuer {
    DatagramBindingIssuer::new(ComponentInstanceId {
        component: ComponentId(1),
        generation: ResourceGeneration(1),
    })
}
fn request(issuer: &DatagramBindingIssuer) -> BoundDatagramRequest {
    BoundDatagramRequest {
        binding: issuer.issue(0),
        peer: "127.0.0.1:9".parse().unwrap(),
        expected_network_generation: NetworkGeneration(0),
    }
}
fn error() -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
}
fn owner() -> ControlledDatagramOwner {
    ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap())
}

#[test]
fn actual_no_native_evidence_rejects_another_owner_mutated_diagnostics_and_other_issuer() {
    let issuer = issuer();
    let request = request(&issuer);
    let mut actual = owner();
    let selected = EgressInterfaceControl::default()
        .select_for_peer_with_policy(request.peer, &DialPolicy::default())
        .unwrap();
    let invalid = DialPolicy {
        source_ip: Some("192.0.2.197".parse().unwrap()),
        ..Default::default()
    };
    let mut acquired = actual
        .acquire(request.peer, None, &invalid, &selected)
        .unwrap_err();
    assert!(actual.confirms_settled_acquisition(&acquired));
    assert!(!owner().confirms_settled_acquisition(&acquired));
    let original = acquired.source.clone();
    acquired.source = Arc::new(std::io::Error::other("public diagnostic replacement"));
    assert!(!actual.confirms_settled_acquisition(&acquired));
    acquired.source = original.clone();
    let old = acquired.reservation;
    acquired.reservation = None;
    assert!(!actual.confirms_settled_acquisition(&acquired));
    acquired.reservation = old;
    let failed = issuer.acquisition_failure(&request, error(), acquired, &actual);
    assert!(Arc::ptr_eq(
        &failed
            .error()
            .source()
            .unwrap()
            .downcast_ref::<DatagramAcquisitionError>()
            .unwrap()
            .source,
        &original
    ));
    let (diagnostic, receipt) = failed.into_parts();
    let receipt = receipt.expect("actual original never acquired native I/O");
    assert!(receipt.matches(&request));
    let other_issuer = super::tests::issuer();
    assert!(!receipt.matches(&super::tests::request(&other_issuer)));
    let mut other_peer = request.clone();
    other_peer.peer.set_port(10);
    assert!(!receipt.matches(&other_peer));
    let mut other_generation = request.clone();
    other_generation.expected_network_generation = NetworkGeneration(1);
    assert!(!receipt.matches(&other_generation));
    assert!(diagnostic.source().is_some());
    assert!(actual.close_all().is_complete());
}

#[test]
fn real_registration_panic_and_mutated_unknown_cannot_mint_a_retry_receipt() {
    let issuer = issuer();
    let request = request(&issuer);
    let mut actual = owner();
    let selected = EgressInterfaceControl::default()
        .select_for_peer_with_policy(request.peer, &DialPolicy::default())
        .unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = actual.acquire(request.peer, None, &DialPolicy::default(), &selected);
    }));
    assert!(
        panic.is_err(),
        "actual registration requires a Tokio reactor"
    );
    let mut unknown = actual
        .acquire(request.peer, None, &DialPolicy::default(), &selected)
        .unwrap_err();
    assert!(matches!(
        unknown.outcome,
        crate::controlled_datagram::AcquisitionOutcome::Unknown(_)
    ));
    unknown.outcome = crate::controlled_datagram::AcquisitionOutcome::KnownNoNative;
    assert!(!actual.confirms_settled_acquisition(&unknown));
    let (_, receipt) = issuer
        .acquisition_failure(&request, error(), unknown, &actual)
        .into_parts();
    assert!(receipt.is_none());
    assert_eq!(actual.occupied(), 1);
    assert!(!actual.close_all().is_complete());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn actual_native_bind_failure_needs_the_same_reaped_owner_before_receipt() {
    let issuer = issuer();
    let mut request = request(&issuer);
    request.peer = "192.0.2.197:9".parse().unwrap();
    let egress = EgressInterfaceControl::default();
    egress.replace(Some(
        crate::EgressInterface::new("bound-attempt-interface-longer-than-ifnamsiz", u32::MAX)
            .unwrap(),
    ));
    let selected = egress
        .select_for_peer_with_policy(request.peer, &DialPolicy::default())
        .unwrap();
    let mut actual = owner();
    let acquired = actual
        .acquire(request.peer, None, &DialPolicy::default(), &selected)
        .unwrap_err();
    assert!(matches!(
        acquired.outcome,
        crate::controlled_datagram::AcquisitionOutcome::Closed(_)
    ));
    assert!(acquired.to_string().contains("bind_interface"));
    let original = acquired.source.clone();
    assert_eq!(actual.occupied(), 0);
    let report = actual.take_report();
    assert_eq!(report.confirmed_closed, 1);
    assert!(report.is_complete());
    assert!(!owner().confirms_settled_acquisition(&acquired));
    let failed = issuer.acquisition_failure(&request, error(), acquired, &actual);
    assert!(Arc::ptr_eq(
        &failed
            .error()
            .source()
            .unwrap()
            .downcast_ref::<DatagramAcquisitionError>()
            .unwrap()
            .source,
        &original
    ));
    assert!(failed.into_parts().1.unwrap().matches(&request));
}
