//! Real caller/observer memory custody, not ordinary Source qualification.
use super::*;
use crate::{
    contract::scope::TaskScope,
    controlled_carrier::{CarrierOriginalTotals, LogicalPacketProviderRegistration},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::component::{
    ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration,
};
#[tokio::test]
async fn actual_source_pending_observer_keeps_header_credit_after_its_record_returns() {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(NonZeroUsize::MIN));
    scope.enable_registration().unwrap();
    let tasks = scope.registrar();
    let (mut stream, mut peer) = tokio::io::duplex(1);
    let (entered, pending) = tokio::sync::oneshot::channel();
    let (_, driver) = tasks
        .spawn_tracked(
            crate::contract::lifecycle::TaskClass::Critical,
            async move {
                let mut byte = [0];
                let reading = stream.read_exact(&mut byte);
                tokio::pin!(reading);
                std::future::poll_fn(|cx| {
                    assert!(std::future::Future::poll(reading.as_mut(), cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                entered.send(()).unwrap();
                reading
                    .await
                    .map_err(|source| failure(Reason::Io).with_source(source))?;
                Ok(())
            },
        )
        .unwrap();
    pending.await.unwrap();
    let software = LogicalPacketProviderRegistration::from_registered(
        crate::contract::component::PacketDeviceIdentity::new(
            ComponentInstanceId {
                component: ComponentId(9137),
                generation: ResourceGeneration(1),
            },
            tasks.scope_identity(),
        ),
        driver.clone(),
        tasks.cancellation(),
    )
    .unwrap();
    let total = CarrierOriginalTotals::new(NonZeroUsize::MIN);
    software
        .reserve_original_totals(&tasks.downgrade(), &total)
        .unwrap();
    let original = CarrierOrigin::from_logical_packet(software);
    let memory = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let calls = PacketDriverCalls::from_registered(original.clone(), &tasks.downgrade()).unwrap();
    calls
        .install_send_workspaces(
            &original,
            vec![(
                driver.clone(),
                memory.native_grant(255).unwrap(),
                memory.native_grant(255).unwrap(),
            )],
        )
        .unwrap();
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(1),
    };
    let baseline = memory.driver_metadata().occupied;
    let table_layout =
        zero_traits::packet_metadata::PacketMetadataLayout::array::<RecordRef>(1).unwrap();
    let table_baseline = baseline.checked_add(table_layout).unwrap();
    let expected = crate::contract::network::UpstreamTransport::packet_driver_opener_layout::<
        pending::Observer,
    >(&binding)
    .unwrap()
    .checked_add(table_baseline)
    .unwrap();
    let grant = calls
        .register(
            &driver,
            binding,
            CarrierContext {
                session: None,
                deadline: None,
                cancel: crate::contract::CancellationToken::new(),
            },
            0,
            Arc::from([]),
            Arc::from([]),
        )
        .unwrap();
    let identity = grant.identity();
    let other_identity = RequestIdentity::Service(
        Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::BackgroundControl,
        })
        .into(),
    );
    let waiting = grant.pending_observer();
    let another = waiting.clone();
    assert!(
        waiting
            .packet_caller(&other_identity, &grant.call())
            .is_err(),
        "equal facts are not the original Paid caller owner"
    );
    assert!(waiting
        .packet_caller(&identity, &grant.call())
        .unwrap()
        .same_registration(&driver));
    assert_eq!(
        memory.driver_metadata().occupied,
        expected
            .checked_add(RecordRef::layout().unwrap())
            .unwrap()
            .checked_add(crate::contract::context::ServiceFactsOwner::paid_layout().unwrap())
            .unwrap()
            .checked_add(SettledRef::layout().unwrap())
            .unwrap()
            .checked_add(InstalledRef::layout().unwrap())
            .unwrap()
            .checked_add(zero_traits::packet_metadata::PacketMetadataLayout::boxed::<
                control::StoredControl,
            >())
            .unwrap()
    );
    let facts_layout = crate::contract::context::ServiceFactsOwner::paid_layout().unwrap();
    drop(grant);
    calls.collect_retired();
    assert!(calls.calls.lock().unwrap().is_empty());
    assert_eq!(
        waiting.capacity(),
        None,
        "the actual observer body/record no longer exists"
    );
    assert!(!driver.is_joined());
    assert_eq!(
        memory.driver_metadata().occupied,
        expected.checked_add(facts_layout).unwrap(),
        "both still-live actual Weak headers retain the original allocation credit"
    );
    drop(waiting);
    assert_eq!(
        memory.driver_metadata().occupied,
        expected.checked_add(facts_layout).unwrap()
    );
    drop(another);
    assert_eq!(
        memory.driver_metadata().occupied,
        table_baseline.checked_add(facts_layout).unwrap(),
        "the actual identity still owns the same allocation after its Record returned"
    );
    drop(identity);
    assert_eq!(
        memory.driver_metadata().occupied,
        table_baseline,
        "empty Calls still owns its real allocated row capacity"
    );
    drop(calls);
    assert_eq!(
        memory.driver_metadata().occupied,
        baseline,
        "only actual table deallocation returns its capacity"
    );
    assert_eq!(
        total.snapshot().charged,
        1,
        "memory release is not whole Source Scope release"
    );
    scope.begin_shutdown();
    peer.write_all(&[1]).await.unwrap();
    let exit = scope.next_receipt().await.unwrap();
    assert!(driver.matches_joined(exit.joined()));
    assert_eq!(total.snapshot().charged, 1);
    scope.acknowledge(&exit).unwrap();
    driver.wait_registration_released().await;
    assert_eq!(total.snapshot().charged, 0);
    assert!(original.closed().is_none());
}
