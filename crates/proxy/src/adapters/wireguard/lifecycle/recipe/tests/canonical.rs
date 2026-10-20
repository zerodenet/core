use super::*;
use crate::component::{
    catalog::{supervision::testing::TestContext, ComponentCatalog},
    instance::ComponentSlot,
    ResourcePreparation,
};
use std::{future::Future, task::Poll};
use zero_core::component::ResourceGeneration;

fn slot(
    catalog: &ComponentCatalog,
    recipe: LinkedEndpointRecipe,
) -> (Arc<ComponentSlot>, LinkedInvocation) {
    let mut allocate = || catalog.allocate_id();
    let mut preparation = ResourcePreparation::new(&mut allocate);
    let (id, invocation) = recipe.into_component(&mut preparation).unwrap();
    let order = preparation.validate(std::iter::empty()).unwrap();
    assert_eq!(order, vec![id]);
    let resources = preparation.into_resources().unwrap();
    assert_eq!(resources.len(), 1);
    let prepared = &resources[0];
    let spec = prepared.recipe().spec();
    assert!(spec.roles.is_empty());
    assert!(spec.resources.is_empty()); // virtual stacks own no listener socket
    assert_eq!(
        spec.task_limit.unwrap().get(),
        crate::runtime::raw_ip::EndpointStackOwner::TASK_LIMIT
    );
    (
        Arc::new(ComponentSlot::from_recipe(
            id,
            ResourceGeneration(1),
            prepared.recipe().clone(),
            Default::default(),
            "linked_peers_start",
        )),
        invocation,
    )
}
#[test]
fn canonical_graph_preparation_is_cold_in_the_actual_host_allocation_domain() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let adapter = WireguardAdapter::default();
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    let recipe = recipe(&adapter, "127.0.0.1:51821", egress.generation());
    let pending = recipe.pending();
    let catalog = ComponentCatalog::default();
    let (slot, _invocation) = slot(&catalog, recipe);
    assert!(!pending.needs_cleanup());
    assert!(pending.ready().is_none());
    assert!(pending.peer_stack(0).unwrap().device().is_none());
    // A second identity still comes from this very host allocator; no locally
    // invented component/generation ID or second namespace manager is used.
    assert!(catalog.allocate_id().unwrap().0 > slot.instance_id().component.0);
}
#[tokio::test]
async fn same_canonical_slot_registers_the_original_workers_and_confirms_them_before_replacement() {
    let proxy = crate::Proxy::new(proxy_config()).unwrap();
    let upstream = proxy.tcp_runtime_services().upstream();
    let adapter = WireguardAdapter::default();
    let catalog = ComponentCatalog::default();
    let recipe = recipe(&adapter, "127.0.0.1:51821", upstream.egress_generation());
    let pending = recipe.pending();
    let guard = install(&adapter, &recipe);
    let request = demand(&recipe);
    let (slot, invocation) = slot(&catalog, recipe);
    let host = TestContext::new([slot.as_ref()]);
    let resolved = invocation.resolve(&upstream, request).await.unwrap();
    assert!(pending.peer_stack(0).unwrap().device().is_none());
    host.drive(slot.start(deadline())).await.unwrap();
    let link = resolved.install().unwrap();
    let device = link.state.lock().unwrap().devices[0].clone();
    let (sender, mut packets) = tokio::sync::mpsc::channel(4);
    let previous = link.sender.send_replace(sender);
    drop(previous);
    let mut connecting = Box::pin(device.open_tcp_observed(
        "10.0.0.1".parse().unwrap(),
        "192.0.2.2:443".parse().unwrap(),
        None,
    ));
    std::future::poll_fn(|cx| {
        assert!(connecting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    // The original endpoint driver really submits SYN through this Link's
    // actual sender watch. No marker or elapsed delay stands in for delivery.
    let syn = tokio::time::timeout_at(deadline(), packets.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(zero_stack::packet::parse_tcp(&syn.packet).unwrap().syn);
    let report = host.drive(slot.shutdown(deadline())).await.unwrap();
    assert!(report.is_complete(), "{:?}", report.unfinished);
    assert_eq!(report.exited_tasks.len(), 3);
    assert!(report.failed_tasks.is_empty());
    assert!(report.closed_io.is_empty()); // listener physical receipt is separate
    assert!(device.is_closed());
    assert!(pending.peer_stack(0).unwrap().is_drained());
    assert!(connecting.await.is_err());
    assert!(PendingLinkedEndpoint::from_ready(link.clone()).is_err());
    let live = pending.live();
    drop(guard);
    assert!(adapter.pending_endpoints.lock().unwrap().is_none());
    let mut next_inbound = inputs("127.0.0.1:51821").1;
    next_inbound.listen.port += 1;
    let outbound = inputs("127.0.0.1:51821").0;
    let next = LinkedEndpointRecipe::prepare(
        &adapter,
        &outbound,
        &next_inbound,
        Arc::new(WireguardRawIpPlan::from_protocol(&outbound.protocol).unwrap()),
        LinkedRecipeSettings {
            identity: link.state.lock().unwrap().identity,
            network_generation: NetworkGeneration(upstream.egress_generation()),
            stacks: vec![Default::default()],
        },
    )
    .unwrap();
    assert!(Arc::ptr_eq(&next.pending().live(), &live));
    let next_pending = next.pending();
    let next_guard = install(&adapter, &next);
    let request = demand(&next);
    let (next_slot, invocation) = self::slot(&catalog, next);
    assert_ne!(next_slot.instance_id(), slot.instance_id());
    let next_host = TestContext::new([next_slot.as_ref()]);
    let resolved = invocation.resolve(&upstream, request).await.unwrap();
    next_host.drive(next_slot.start(deadline())).await.unwrap();
    let next_link = resolved.install().unwrap();
    let next_device = next_link.state.lock().unwrap().devices[0].clone();
    assert!(!Arc::ptr_eq(&next_device, &device));
    assert!(Arc::ptr_eq(&next_link.live, &live));
    assert!(next_host
        .drive(next_slot.shutdown(deadline()))
        .await
        .unwrap()
        .is_complete());
    assert!(next_pending.peer_stack(0).unwrap().is_drained());
    drop(next_guard);
}

#[tokio::test]
async fn interrupted_slot_cleanup_retains_the_original_graph_until_the_same_inline_join_consumer_runs(
) {
    let proxy = crate::Proxy::new(proxy_config()).unwrap();
    let upstream = proxy.tcp_runtime_services().upstream();
    let adapter = WireguardAdapter::default();
    let catalog = ComponentCatalog::default();
    let recipe = recipe(&adapter, "127.0.0.1:51821", upstream.egress_generation());
    let pending = recipe.pending();
    let guard = install(&adapter, &recipe);
    let expected = adapter
        .pending_endpoints
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    let request = demand(&recipe);
    let (slot, invocation) = slot(&catalog, recipe);
    let host = TestContext::new([slot.as_ref()]);
    let resolved = invocation.resolve(&upstream, request).await.unwrap();
    host.drive(slot.start(deadline())).await.unwrap();
    let witnesses = pending.peer_stack(0).unwrap().test_original_witnesses();
    assert_eq!(witnesses.len(), 2); // original control + endpoint driver
    assert!(witnesses.iter().all(|witness| !witness.is_joined()));
    // The slot owns the real graph even though installation into a Link never
    // occurs. Abandonment revokes it, but cannot refund these original tasks.
    drop(resolved);
    drop(guard);
    assert!(pending.needs_cleanup());
    assert!(PendingEndpoints::release_closed(&adapter.pending_endpoints, &expected).is_err());
    let mut joining = Box::pin(slot.shutdown(deadline()));
    std::future::poll_fn(|cx| {
        assert!(joining.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(joining);
    assert!(!pending.peer_stack(0).unwrap().is_drained());
    let report = host.drive(slot.shutdown(deadline())).await.unwrap();
    assert!(report.is_complete());
    // Dropping the uninstalled invocation releases its actual packet receiver.
    // The endpoint driver may then exit Ok while the slot is still running;
    // the host deliberately ACKs that success without retaining history. Its
    // original witness still proves the exact join, independently of the
    // closing-phase report (which can therefore contain only the control).
    let joined: Vec<_> = witnesses
        .iter()
        .map(|witness| {
            witness
                .try_joined()
                .expect("same inline host consumer must join both original tasks")
        })
        .collect();
    assert!(joined.iter().all(|joined| joined.result().is_ok()));
    assert_ne!(joined[0].id(), joined[1].id());
    assert!(report
        .exited_tasks
        .iter()
        .all(|id| joined.iter().any(|joined| joined.id() == *id)));
    assert!(pending.peer_stack(0).unwrap().is_drained());
    PendingEndpoints::release_closed(&adapter.pending_endpoints, &expected).unwrap();
    assert!(adapter.pending_endpoints.lock().unwrap().is_none());
}

#[tokio::test]
async fn actual_cold_union_and_ordinary_service_start_cannot_grant_deferred_graph_start_or_commit()
{
    let adapter = WireguardAdapter::default();
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    let recipe = recipe(&adapter, "127.0.0.1:51821", egress.generation());
    let pending = recipe.pending();
    let guard = install(&adapter, &recipe);
    let catalog = ComponentCatalog::default();
    let config = Arc::new(proxy_config());
    let mut actual_id = None;
    let mut invocation = None;
    let plan = catalog
        .prepare_with_device_resources(config.clone(), |preparation| {
            let (id, operation) = recipe.into_component(preparation).map_err(|source| {
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::Io,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(source)
            })?;
            actual_id = Some(id);
            invocation = Some(operation);
            Ok(std::collections::BTreeSet::from([id]))
        })
        .unwrap();
    let slot = plan.device_component(actual_id.unwrap()).unwrap();
    assert!(Arc::ptr_eq(
        &plan,
        &catalog.prepare(config.clone()).unwrap()
    ));
    assert!(slot.recipe.spec().roles.is_empty());
    catalog.start(config.clone(), deadline()).await.unwrap();
    // This is the actual bound HostAdmission, not a fake registrar or a
    // temporary scope. The cold virtual-device node is absent from its grants.
    assert!(slot.start(deadline()).await.is_err());
    assert!(pending.peer_stack(0).unwrap().device().is_none());
    assert!(!pending.needs_cleanup());
    assert!(catalog.commit(&config, || Ok(())).is_err());
    catalog.shutdown(deadline()).await.unwrap();
    drop(invocation);
    drop(guard);
    assert!(adapter.pending_endpoints.lock().unwrap().is_none());
}
