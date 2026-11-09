//! Explicit library software Source; real protocol IO and hosted actor ACK.
//! This is no Native-close or ordinary catalog publication fixture.
use super::*;
fn has_actual_source(error: &ChannelError, actual: &Arc<ChannelError>) -> bool {
    let mut source: &(dyn std::error::Error + 'static) = error;
    loop {
        if source
            .downcast_ref::<ChannelError>()
            .is_some_and(|same| std::ptr::eq(same, actual.as_ref()))
        {
            return true;
        }
        let Some(next) = source.source() else {
            return false;
        };
        source = next;
    }
}
use crate::{
    contract::{component::PacketDeviceIdentity, CancellationToken},
    contract::{
        network::UpstreamBinding,
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
#[path = "recovery_io.rs"]
mod io;
#[tokio::test]
async fn actual_actor_failure_keeps_new_receipt_and_requires_source_supervision_after_host_ack() {
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(2));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let cancel = tasks.cancellation();
        let (mut wire, mut peer) = tokio::io::duplex(8);
        let (ready, pending) = tokio::sync::oneshot::channel();
        let (_, witness) = tasks
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
                    .map_err(|e| failure(Reason::Io).with_source(e))?;
                Ok(())
            })
            .unwrap();
        pending.await.unwrap();
        let instance = ComponentInstanceId {
            component: ComponentId(489),
            generation: ResourceGeneration(1),
        };
        let software = LogicalPacketProviderRegistration::from_registered(
            PacketDeviceIdentity::new(instance, tasks.scope_identity()),
            witness.clone(),
            cancel,
        )
        .unwrap();
        let total = CarrierOriginalTotals::new(NonZeroUsize::MIN);
        software
            .reserve_original_totals(&tasks.downgrade(), &total)
            .unwrap();
        let original = CarrierOrigin::from_logical_packet(software);
        let root = Arc::new(
            PacketDriverCalls::from_registered(original.clone(), &tasks.downgrade()).unwrap(),
        );
        #[cfg(feature = "native-packet-origin")]
        {
            use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
            let memory = Arc::new(PacketReturnTotals::new_with_transmit(
                PacketMemoryLimit {
                    frames: 2,
                    bytes: 512,
                },
                PacketMemoryLimit {
                    frames: 2,
                    bytes: 512,
                },
                PacketMemoryLimit {
                    frames: 2,
                    bytes: 512,
                },
            ));
            root.install_send_workspaces(
                &original,
                vec![(
                    witness.clone(),
                    memory.native_grant(255).unwrap(),
                    memory.native_grant(255).unwrap(),
                )],
            )
            .unwrap();
        }
        let actor = io::malformed_actor(&root, &tasks, &witness).await;
        let exit = scope.next_receipt().await.unwrap();
        assert!(actor.matches_joined(exit.joined()));
        let actual = exit.result().unwrap_err();
        let report = Arc::new(root.receive_actor_failures().unwrap());
        assert!(actor.try_joined().unwrap().failure_accepted());
        assert!(has_actual_source(&report, &actual));
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(1),
        };
        let call = || CarrierContext {
            session: None,
            deadline: None,
            cancel: CancellationToken::new(),
        };
        let register = || {
            root.register(
                &witness,
                binding.clone(),
                call(),
                0,
                Arc::from([]),
                Arc::from([]),
            )
        };
        assert!(
            register().is_err(),
            "unreceived original fault cannot be cleared"
        );
        // This fixed receiver row retains the exact issued Arc, not its string.
        let receiver = OnceLock::new();
        receiver.set(report.clone()).unwrap();
        let receipt = report.cleanup_failure_receipt().unwrap();
        assert!(receipt.acknowledge_retained(receiver.get().unwrap()));
        assert!(
            register().is_err(),
            "actual actor registration is not yet ACKed"
        );
        let callbacks = scope.defer_callbacks();
        scope.acknowledge(&exit).unwrap();
        assert!(!actor.try_joined().unwrap().registration_released());
        assert!(register().is_err());
        drop(callbacks);
        actor.wait_registration_released().await;
        assert!(
            register().is_err(),
            "fatal actor failure cannot become a healthy live caller after ACK"
        );
        assert!(has_actual_source(&report, &actual));
        assert_eq!(total.snapshot().charged, 1);
        // Error delivery and real Hosted ACK are separate from whole Source
        // retirement. The actual Source supervisor must rebuild this owner.
        let completed = root
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(completed.is_complete());
        let failure = completed.resource_failures.first().unwrap();
        assert_eq!(failure.instance, instance);
        assert!(std::ptr::eq(
            failure.source.as_ref(),
            report.retained_source().unwrap().as_ref(),
        ));
        assert!(failure
            .source
            .cleanup_failure_receipt()
            .unwrap()
            .same_receipt(receipt));
        assert!(has_actual_source(&failure.source, &actual));
        let repeated = root
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(
            repeated.resource_failures.first().unwrap(),
            failure
        ));
        scope.begin_shutdown();
        peer.write_all(&[1]).await.unwrap();
        let wire_exit = scope.next_receipt().await.unwrap();
        assert!(witness.matches_joined(wire_exit.joined()));
        scope.acknowledge(&wire_exit).unwrap();
        assert_eq!(total.snapshot().charged, 0);
        assert!(
            original.closed().is_none(),
            "software original never certifies physical close"
        );
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}

#[path = "recovery_late_actor.rs"]
mod late_actor;
