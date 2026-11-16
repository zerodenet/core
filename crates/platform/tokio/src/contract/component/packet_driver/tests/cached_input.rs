//! Actual cached TCP input survives a failed pre-Datagram Source preparation.
//! Logical return is checked separately from this donor's native close/Scope.
use super::*;
use crate::{
    contract::{
        component::{PacketDeviceIdentity, ResourceDeclaration},
        lifecycle::ResourceLifecycle,
        network::UpstreamOpener,
        scope::{TaskClass, TaskScope},
        CancellationToken,
    },
    controlled_carrier::{
        CarrierOriginalTotals, CarrierOwner, LogicalPacketProviderRegistration, NativeCarrierOpener,
    },
};
use std::{
    future::{poll_fn, Future},
    task::Poll,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{
    component::{ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration},
    request::Destination,
    Address,
};

fn end() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[tokio::test]
async fn failed_preparation_returns_only_its_original_cached_input_without_closing_the_donor() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let mut source_scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(NonZeroUsize::MIN));
        source_scope.enable_registration().unwrap();
        let tasks = source_scope.registrar();
        let (mut wire, mut wire_peer) = tokio::io::duplex(1);
        let (at_io, pending) = tokio::sync::oneshot::channel();
        let (_, wire_task) = tasks
            .spawn_tracked(TaskClass::Critical, async move {
                let mut byte = [0];
                let reading = wire.read_exact(&mut byte);
                tokio::pin!(reading);
                poll_fn(|cx| {
                    assert!(reading.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                at_io.send(()).unwrap();
                reading
                    .await
                    .map_err(|source| failure(Reason::Io).with_source(source))?;
                Ok(())
            })
            .unwrap();
        pending.await.unwrap();
        let source_id = ComponentInstanceId {
            component: ComponentId(496),
            generation: ResourceGeneration(1),
        };
        let source = LogicalPacketProviderRegistration::from_registered(
            PacketDeviceIdentity::new(source_id, tasks.scope_identity()),
            wire_task.clone(),
            tasks.cancellation(),
        )
        .unwrap();
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(2).unwrap());
        source
            .reserve_original_totals(&tasks.downgrade(), &totals)
            .unwrap();
        let source = CarrierOrigin::from_logical_packet(source);
        let calls = PacketDriverCalls::from_registered(source.clone(), &tasks.downgrade()).unwrap();
        let memory = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
        calls
            .install_send_workspaces(
                &source,
                vec![(
                    wire_task.clone(),
                    memory.native_grant(255).unwrap(),
                    memory.native_grant(255).unwrap(),
                )],
            )
            .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let endpoint = Destination::new(Address::Ipv4([127, 0, 0, 1]), remote.port());
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let donor_id = ComponentInstanceId {
            component: ComponentId(497),
            generation: ResourceGeneration(1),
        };
        let host = Arc::new(CancellationToken::new());
        let mut donor_scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(NonZeroUsize::MIN));
        donor_scope.enable_registration().unwrap();
        let mut donor = CarrierOwner::new(donor_id, NonZeroUsize::MIN, &host);
        let owner = donor.registration();
        owner
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledCarrierPool {
                    endpoint: endpoint.clone(),
                    dial: Default::default(),
                    datagram: false,
                    max_sockets: NonZeroUsize::MIN,
                },
                &donor_scope.registrar(),
                &host,
                &totals,
            )
            .unwrap();
        donor.start(end()).await.unwrap();
        let preparing = calls
            .register(
                &wire_task,
                binding.clone(),
                CarrierContext {
                    session: None,
                    deadline: Some(end()),
                    cancel: CancellationToken::new(),
                },
                1,
                Arc::from([owner.clone()]),
                Arc::from([]),
            )
            .unwrap();
        let record = preparing.record.clone();
        let mut native = donor.grants().reserve().unwrap();
        let view = native.claim_driver_view().unwrap();
        let original = native.adoption();
        preparing
            .pending_observer()
            .retain(original.clone())
            .unwrap();
        let opener =
            NativeCarrierOpener::new(donor_id, binding.clone(), remote, egress, None).unwrap();
        let io = opener
            .prepare_controlled_stream(&binding, endpoint, preparing.call().clone(), native)
            .unwrap()
            .open(end())
            .await
            .unwrap()
            .into_parts()
            .0;
        let (mut peer, _) = listener.accept().await.unwrap();
        let (at_io, pending) = tokio::sync::oneshot::channel();
        let (_, native_task) = original
            .spawn_driver(
                &donor_scope.registrar(),
                TaskClass::Critical,
                io.into_driver(),
                move |mut io| async move {
                    let _view = view;
                    let mut byte = [0];
                    let reading = io.read_exact(&mut byte);
                    tokio::pin!(reading);
                    poll_fn(|cx| {
                        assert!(reading.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    at_io.send(()).unwrap();
                    reading
                        .await
                        .map_err(|source| failure(Reason::Io).with_source(source))?;
                    Ok(())
                },
            )
            .unwrap();
        pending.await.unwrap();
        assert!(original.retained_driver().unwrap().retains(&original));
        assert!(record.installed.get().is_none());
        record.request_retirement();
        assert!(
            record.pending.lock().unwrap().is_empty(),
            "the exact live cached driver received its input before caller cancellation"
        );
        assert!(
            !record.released(),
            "the same preparing grant can still return a late logical controller"
        );
        calls.collect_retired();
        assert_eq!(calls.calls.lock().unwrap().len(), 1);
        assert!(original.closed().is_none() && original.users_closed().is_none());
        assert_eq!(totals.snapshot().charged, 2);
        drop(preparing);
        calls.collect_retired();
        assert!(calls.calls.lock().unwrap().is_empty());
        assert!(record.released());
        assert!(!native_task.is_joined() && !wire_task.is_joined());
        assert!(
            original.closed().is_none(),
            "return to an actual cached donor is not NativeClosed"
        );
        peer.write_all(&[1]).await.unwrap();
        let joined = donor_scope.next_receipt().await.unwrap();
        assert!(native_task.matches_joined(joined.joined()));
        joined.joined().result().unwrap();
        donor_scope.acknowledge(&joined).unwrap();
        donor_scope.begin_shutdown();
        assert!(donor.shutdown(end()).await.unwrap().is_complete());
        let closed = original.closed().unwrap();
        assert!(closed.matches(&original) && closed.had_native_acquisition());
        assert!(original.users_closed().unwrap().matches(&original));
        assert!(owner.closed().unwrap().matches(&owner));
        assert!(donor_scope.is_drained());
        source_scope.begin_shutdown();
        wire_peer.write_all(&[1]).await.unwrap();
        let joined = source_scope.next_receipt().await.unwrap();
        assert!(wire_task.matches_joined(joined.joined()));
        joined.joined().result().unwrap();
        source_scope.acknowledge(&joined).unwrap();
        assert!(source_scope.is_drained());
    })
    .await
    .unwrap();
}
