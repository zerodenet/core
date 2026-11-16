//! Invocation isolation with actual pending Source/actor IO and hosted ACKs.
//! These metadata assertions do not qualify a physical provider's native close.
use super::*;
use crate::{
    contract::{
        component::PacketDeviceIdentity,
        scope::{TaskClass, TaskScope},
    },
    controlled_carrier::{CarrierOriginalTotals, LogicalPacketProviderRegistration},
};
use std::{
    future::{poll_fn, Future},
    task::Poll,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::component::{
    ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration,
};

fn register(
    root: &PacketDriverCalls,
    caller: &TaskJoinWitness,
    lineage: MetadataRef<Lineage>,
) -> PacketDriverCallGrant {
    root.register_with_protocol_targets(
        caller,
        UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(1),
        },
        CarrierContext {
            session: None,
            deadline: None,
            cancel: crate::contract::CancellationToken::new(),
        },
        super::super::registration::OriginalTargets {
            lineage: Some(lineage),
            max_pending: 0,
            allowed: Arc::from([]),
            packet_originals: Arc::from([]),
            protocols: None,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn peer_retirement_isolated_and_failed_setup_history_survives_reconnect_actor_reclamation() {
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(4));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let (mut wire, mut input) = tokio::io::duplex(8);
        let (ready, pending) = tokio::sync::oneshot::channel();
        let (_, main) = tasks
            .spawn_tracked(TaskClass::Critical, async move {
                let mut byte = [0];
                let reading = wire.read_exact(&mut byte);
                tokio::pin!(reading);
                poll_fn(|cx| {
                    assert!(reading.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                ready.send(()).unwrap();
                reading
                    .await
                    .map_err(|source| failure(Reason::Io).with_source(source))?;
                Ok(())
            })
            .unwrap();
        pending.await.unwrap();
        let original = LogicalPacketProviderRegistration::from_registered(
            PacketDeviceIdentity::new(
                ComponentInstanceId {
                    component: ComponentId(495),
                    generation: ResourceGeneration(1),
                },
                tasks.scope_identity(),
            ),
            main.clone(),
            tasks.cancellation(),
        )
        .unwrap();
        let totals = CarrierOriginalTotals::new(NonZeroUsize::MIN);
        original
            .reserve_original_totals(&tasks.downgrade(), &totals)
            .unwrap();
        let original = CarrierOrigin::from_logical_packet(original);
        let root = Arc::new(
            PacketDriverCalls::from_registered(original.clone(), &tasks.downgrade()).unwrap(),
        );
        #[cfg(feature = "native-packet-origin")]
        {
            let memory = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
            root.install_send_workspaces(
                &original,
                vec![(
                    main.clone(),
                    memory.native_grant(255).unwrap(),
                    memory.native_grant(255).unwrap(),
                )],
            )
            .unwrap();
        }
        let first = root.new_lineage(&main).unwrap();
        let second = root.new_lineage(&main).unwrap();
        let a = register(&root, &main, first.clone());
        let b = register(&root, &main, second.clone());
        root.request_lineage_retirement(&first);
        assert!(a.record.call.cancel.is_cancelled());
        assert!(
            !b.record.call.cancel.is_cancelled(),
            "peers share a wire, not a call lifetime"
        );
        assert!(
            !root
                .retire_lineage_attempts(&first, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .is_complete(),
            "a still-held preparing grant can return another original controller"
        );
        assert_eq!(root.calls.lock().unwrap().len(), 2);
        drop(a);
        assert!(root
            .retire_lineage_attempts(&first, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .is_complete());
        assert_eq!(root.calls.lock().unwrap().len(), 1);

        let third = root.new_lineage(&main).unwrap();
        let captured = root.clone();
        let history = third.clone();
        let (send, received) = tokio::sync::oneshot::channel();
        let (at_io, pending) = tokio::sync::oneshot::channel();
        let (mut io, mut peer) = tokio::io::duplex(8);
        let (_, actor) = root
            .spawn_reconnect(&tasks.downgrade(), &main, move |witness| async move {
                send.send(register(&captured, &witness, history))
                    .unwrap_or_else(|_| panic!("actual history receiver ended"));
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
            })
            .unwrap();
        let interrupted_setup = received.await.unwrap();
        pending.await.unwrap();
        assert!(!actor.is_joined());
        peer.write_all(&[1]).await.unwrap();
        let joined = scope.next_receipt().await.unwrap();
        assert!(actor.matches_joined(joined.joined()));
        scope.acknowledge(&joined).unwrap();
        actor.wait_registration_released().await;
        let (_, next) = root
            .spawn_reconnect(&tasks.downgrade(), &main, |_| async { Ok(()) })
            .unwrap();
        let joined = scope.next_receipt().await.unwrap();
        assert!(next.matches_joined(joined.joined()));
        scope.acknowledge(&joined).unwrap();
        next.wait_registration_released().await;
        assert!(
            root.wire_for_actor(&actor).is_err(),
            "the finished actor metadata was actually reclaimed"
        );
        root.request_lineage_retirement(&third);
        assert!(interrupted_setup.record.call.cancel.is_cancelled());
        drop(interrupted_setup);
        assert!(root
            .retire_lineage_attempts(&third, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .is_complete());
        assert!(!b.record.call.cancel.is_cancelled());
        assert!(!main.is_joined());
        assert!(
            original.closed().is_none(),
            "attempt cleanup does not close the shared Source original"
        );
        root.request_lineage_retirement(&second);
        drop(b);
        assert!(root
            .retire_lineage_attempts(&second, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .is_complete());
        assert!(root
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .is_complete());
        scope.begin_shutdown();
        input.write_all(&[1]).await.unwrap();
        let joined = scope.next_receipt().await.unwrap();
        assert!(main.matches_joined(joined.joined()));
        scope.acknowledge(&joined).unwrap();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
