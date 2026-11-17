use super::super::catalog::ComponentCatalog;
use std::sync::{atomic::Ordering, Arc};
use tokio::{
    io::AsyncReadExt,
    time::{Duration, Instant},
};
use zero_config::{OutboundAddressFamily, RuntimeConfig};
use zero_core::{
    component::{InboundId, NetworkGeneration, RoleId, SessionId},
    request::{Destination, RequestFacts, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address, ProtocolType,
};
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    network::{UpstreamBinding, UpstreamTransport},
    stream::{BoxStream, ConnectRequest, DynStreamOutbound},
    CancellationToken, ChannelError, DeliveryState, Reason,
};

#[path = "host_admission/native.rs"]
mod native;
use native::Root;

pub(super) fn config() -> Arc<RuntimeConfig> {
    Arc::new(RuntimeConfig::parse(r#"{"outbounds":[{"tag":"selected","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap())
}
pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
fn identity(business: bool) -> RequestIdentity {
    if !business {
        return RequestIdentity::Service(
            Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::Bootstrap,
            })
            .into(),
        );
    }
    RequestIdentity::Business(Arc::new(RequestFacts {
        session: SessionId(19),
        inbound: RoleId::Inbound(InboundId(1)),
        original_source: None,
        effective_source: None,
        local_endpoint: None,
        original_target: None,
        target: None,
        target_source: None,
        subject: None,
        peer: None,
        protocol: ProtocolType::new("test"),
        sni: None,
        process: None,
    }))
}
async fn open(
    role: &Arc<dyn DynStreamOutbound>,
    root: &Arc<Root>,
    business: bool,
) -> Result<BoxStream, ChannelError> {
    open_with_policy(role, root, business, Default::default()).await
}
async fn open_with_policy(
    role: &Arc<dyn DynStreamOutbound>,
    root: &Arc<Root>,
    business: bool,
    policy: zero_traits::DialPolicy,
) -> Result<BoxStream, ChannelError> {
    let connected = role
        .connect(
            ConnectRequest {
                destination: root.destination(),
            },
            OpenContext {
                identity: identity(business),
                deadline: Some(deadline()),
                cancel: CancellationToken::new(),
                observer: None,
                upstream: UpstreamTransport::new(
                    UpstreamBinding {
                        prefix: vec![],
                        dial: policy,
                        network_generation: NetworkGeneration(root.egress.generation()),
                    },
                    root.clone(),
                ),
            },
        )
        .await?;
    Ok(connected.io)
}

#[tokio::test]
async fn cold_validation_never_grants_admission_and_failed_commit_keeps_current_closed() {
    let catalog = ComponentCatalog::default();
    catalog
        .drive_supervised(tokio::time::timeout(Duration::from_secs(10), async {
            let config = config();
            let plan = catalog.prepare(config.clone()).unwrap();
            let slot = plan.direct(Some("selected")).unwrap();
            assert!(
                slot.stream(deadline()).await.is_err(),
                "cold preparation cannot start its native owner"
            );
            let root = Arc::new(Root::default());
            root.track(&slot);
            assert_eq!(root.calls.load(Ordering::SeqCst), 0);
            assert!(
                catalog.commit(&config, || Ok(())).is_err(),
                "cold prepare is not a start acknowledgement"
            );
            catalog.start(config.clone(), deadline()).await.unwrap();
            let role = slot.stream(deadline()).await.unwrap();
            open(&role, &root, false).await.unwrap();
            let error = open(&role, &root, true).await.err().unwrap();
            assert_eq!(error.reason, Reason::StalePlan);
            assert_eq!(error.delivery, DeliveryState::NotSubmitted);
            assert_eq!(root.calls.load(Ordering::SeqCst), 1);
            assert!(catalog
                .commit::<()>(&config, || Err("actual commit failed".into()))
                .is_err());
            assert!(open(&role, &root, true).await.is_err());
            catalog.commit(&config, || Ok(())).unwrap();
            open(&role, &root, true).await.unwrap();
            catalog.stop_business();
            assert!(open(&role, &root, true).await.is_err());
            open(&role, &root, false).await.unwrap();
            catalog.shutdown(deadline()).await.unwrap();
            root.confirm_peers_closed().await;
            assert!(open(&role, &root, false).await.is_err());
        }))
        .await
        .expect("actual catalog admission operation did not settle");
}

#[tokio::test]
async fn exact_candidate_survives_cold_cache_replacement_and_cached_old_handles_are_fenced() {
    let catalog = ComponentCatalog::default();
    catalog
        .drive_supervised(tokio::time::timeout(Duration::from_secs(10), async {
            let original = config();
            catalog.start(original.clone(), deadline()).await.unwrap();
            catalog.commit(&original, || Ok(())).unwrap();
            let old_slot = catalog
                .prepare(original.clone())
                .unwrap()
                .direct(Some("selected"))
                .unwrap();
            let old = old_slot.stream(deadline()).await.unwrap();
            let root = Arc::new(Root::default());
            root.track(&old_slot);
            let mut transferred = open(&old, &root, true).await.unwrap();

            let mut candidate = (*original).clone();
            candidate.outbounds[0].dial.address_family = OutboundAddressFamily::OnlyIpv4;
            let candidate = Arc::new(candidate);
            let (started, read) =
                tokio::join!(catalog.start(candidate.clone(), deadline()), async move {
                    let read = transferred.read_u8().await;
                    drop(transferred);
                    read
                });
            assert_eq!(
                read.unwrap_err().kind(),
                std::io::ErrorKind::ConnectionAborted
            );
            started.unwrap();
            let selected_slot = catalog
                .prepare(candidate.clone())
                .unwrap()
                .direct(Some("selected"))
                .unwrap();
            root.track(&selected_slot);
            let selected = selected_slot.stream(deadline()).await.unwrap();
            let mut other = (*original).clone();
            other.outbounds[0].dial.address_family = OutboundAddressFamily::OnlyIpv6;
            let other = Arc::new(other);
            let other_plan = catalog.prepare(other.clone()).unwrap();
            let cold_slot = other_plan.direct(Some("selected")).unwrap();
            assert!(cold_slot.stream(deadline()).await.is_err());
            // Cold alternatives may propose the same next generation. Only the actual
            // host-selected owner receives the grant; matching numeric IDs is insufficient.
            assert!(cold_slot.stream(deadline()).await.is_err());
            assert!(catalog.commit(&other, || Ok(())).is_err());
            catalog.commit(&candidate, || Ok(())).unwrap();
            assert!(Arc::ptr_eq(
                &selected,
                &catalog
                    .prepare(candidate.clone())
                    .unwrap()
                    .direct(Some("selected"))
                    .unwrap()
                    .stream(deadline())
                    .await
                    .unwrap()
            ));
            assert!(open(&old, &root, true).await.is_err());
            assert!(open(&old, &root, false).await.is_err());
            assert!(cold_slot.stream(deadline()).await.is_err());
            open_with_policy(
                &selected,
                &root,
                true,
                candidate.outbounds[0].dial.to_policy(),
            )
            .await
            .unwrap();
            let historical = catalog
                .prepare(original)
                .unwrap()
                .direct(Some("selected"))
                .unwrap();
            assert!(historical.stream(deadline()).await.is_err());
            open_with_policy(
                &selected,
                &root,
                true,
                candidate.outbounds[0].dial.to_policy(),
            )
            .await
            .unwrap();
            // The changed shared Direct owner retired its transferred user
            // before start; publication only exposes the exact ready candidate.
            catalog.shutdown(deadline()).await.unwrap();
            root.confirm_peers_closed().await;
        }))
        .await
        .expect("actual catalog admission operation did not settle");
}

#[tokio::test]
async fn committing_current_cannot_discard_another_actual_started_candidate() {
    use std::sync::atomic::AtomicBool;
    let catalog = ComponentCatalog::default();
    catalog
        .drive_supervised(tokio::time::timeout(Duration::from_secs(10), async {
            let current = config();
            catalog.start(current.clone(), deadline()).await.unwrap();
            catalog.commit(&current, || Ok(())).unwrap();
            let original = catalog
                .prepare(current.clone())
                .unwrap()
                .direct(Some("selected"))
                .unwrap();
            let old = original.stream(deadline()).await.unwrap();
            let mut candidate = (*current).clone();
            candidate.outbounds[0].dial.address_family = OutboundAddressFamily::OnlyIpv4;
            let candidate = Arc::new(candidate);
            catalog.start(candidate.clone(), deadline()).await.unwrap();
            let selected_slot = catalog
                .prepare(candidate.clone())
                .unwrap()
                .direct(Some("selected"))
                .unwrap();
            let role = selected_slot.stream(deadline()).await.unwrap();
            let mutated = AtomicBool::new(false);
            assert!(catalog
                .commit(&current, || {
                    mutated.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .is_err());
            assert!(
                !mutated.load(Ordering::SeqCst),
                "reject before any commit mutation"
            );
            let root = Arc::new(Root::default());
            root.track(&selected_slot);
            open_with_policy(&role, &root, false, candidate.outbounds[0].dial.to_policy())
                .await
                .unwrap();
            assert_eq!(
                open(&old, &root, true).await.err().unwrap().reason,
                Reason::StalePlan,
                "StopOld already retired this actual shared owner"
            );
            // The actual recovery path must first confirm candidate retirement. Merely
            // naming the previous current config cannot discard or replace its owner.
            catalog.start(current.clone(), deadline()).await.unwrap();
            assert!(
                open_with_policy(&role, &root, false, candidate.outbounds[0].dial.to_policy())
                    .await
                    .is_err()
            );
            let restored = catalog
                .prepare(current.clone())
                .unwrap()
                .direct(Some("selected"))
                .unwrap();
            assert_eq!(restored.id, original.id);
            assert_eq!(
                restored.generation.0, 3,
                "recovery uses a fresh generation after the retired current and candidate"
            );
            assert!(!Arc::ptr_eq(&restored, &original));
            catalog.commit(&current, || Ok(())).unwrap();
            assert_eq!(
                open(&old, &root, true).await.err().unwrap().reason,
                Reason::StalePlan
            );
            root.track(&restored);
            open(&restored.stream(deadline()).await.unwrap(), &root, true)
                .await
                .unwrap();
            catalog.shutdown(deadline()).await.unwrap();
            root.confirm_peers_closed().await;
        }))
        .await
        .expect("actual catalog admission operation did not settle");
}
