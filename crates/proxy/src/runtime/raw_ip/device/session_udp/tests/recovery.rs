//! Explicit software Source: a real retained UDP send, original continuation,
//! exact fault delivery and one-attempt reuse. No native-close is fabricated.
use super::*;
use std::{future::poll_fn, num::NonZeroUsize, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::component::{
    ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration,
};
use zero_platform_tokio::{
    contract::{
        component::{PacketDeviceIdentity, PacketDriverCalls},
        network::{CarrierContext, UpstreamBinding},
        scope::{TaskClass, TaskScope},
    },
    controlled_carrier::{
        loan::DatagramLoan, CarrierOrigin, CarrierOriginalTotals, LogicalPacketProviderRegistration,
    },
};
use zero_stack::{client_udp::ClientUdpStack, packet::Endpoint, packet_output::PacketSender};
use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
#[path = "retry.rs"]
mod retry;
#[tokio::test]
async fn same_source_failed_phase_continues_original_close_then_reuses_one_attempt_after_receipt() {
    tokio::time::timeout(Duration::from_secs(4), async {
        let root = Arc::new(PacketReturnTotals::new_with_transmit(
            PacketMemoryLimit {
                frames: 3,
                bytes: 1024,
            },
            PacketMemoryLimit {
                frames: 4,
                bytes: 4096,
            },
            PacketMemoryLimit {
                frames: 8,
                bytes: 4096,
            },
        ));
        let read = root.native_grant(128).unwrap();
        let (send, mut output) = tokio::sync::mpsc::channel::<zero_traits::PacketBuffer>(1);
        let send = PacketSender::from(send)
            .with_packet_memory_view(
                read.memory_view().unwrap(),
                zero_stack::packet_output::memory::PacketWireMemory::Native,
            )
            .unwrap();
        let local = "10.0.0.1".parse().unwrap();
        let peer: SocketAddr = "10.0.0.2:53".parse().unwrap();
        let target = Destination {
            address: zero_core::Address::Ipv4([10, 0, 0, 2]),
            port: 53,
        };
        let stack =
            ClientUdpStack::new_with_queue(vec![local], send, 1500, None, Default::default())
                .unwrap();
        let socket = stack.bind(local).unwrap();
        let sending = socket.sender();
        sending
            .send_to(
                b"first",
                Endpoint {
                    ip: peer.ip(),
                    port: peer.port(),
                },
            )
            .await
            .unwrap();
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(2));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let (mut actual_io, mut controller) = tokio::io::duplex(8);
        let (ready, pending) = tokio::sync::oneshot::channel();
        let (resumed, drained) = tokio::sync::oneshot::channel();
        let (_, wire) = tasks
            .spawn_tracked(TaskClass::Critical, async move {
                let sending = sending.send_to(
                    b"second",
                    Endpoint {
                        ip: peer.ip(),
                        port: peer.port(),
                    },
                );
                tokio::pin!(sending);
                poll_fn(|cx| {
                    assert!(sending.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                // The original send stays owned while this same actual worker is
                // suspended in its real IO. Cancellation may wake the send but
                // cannot pretend this body has polled/returned that continuation.
                ready.send(()).unwrap();
                let mut byte = [0];
                actual_io
                    .read_exact(&mut byte)
                    .await
                    .map_err(|e| failure(Stage::Receive, Reason::Io).with_source(e))?;
                assert_eq!(sending.await.unwrap_err().accepted_fragments, 0);
                let reading = actual_io.read_exact(&mut byte);
                tokio::pin!(reading);
                poll_fn(|cx| {
                    assert!(reading.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                resumed.send(()).unwrap();
                reading
                    .await
                    .map_err(|e| failure(Stage::Receive, Reason::Io).with_source(e))?;
                Ok(())
            })
            .unwrap();
        pending.await.unwrap();
        let original = LogicalPacketProviderRegistration::from_registered(
            PacketDeviceIdentity::new(
                ComponentInstanceId {
                    component: ComponentId(987),
                    generation: ResourceGeneration(1),
                },
                tasks.scope_identity(),
            ),
            wire.clone(),
            tasks.cancellation(),
        )
        .unwrap();
        let total = CarrierOriginalTotals::new(NonZeroUsize::MIN);
        original
            .reserve_original_totals(&tasks.downgrade(), &total)
            .unwrap();
        let original = CarrierOrigin::from_logical_packet(original);
        let calls = Arc::new(
            PacketDriverCalls::from_registered(original.clone(), &tasks.downgrade()).unwrap(),
        );
        calls
            .install_send_workspaces(
                &original,
                vec![(
                    wire.clone(),
                    root.native_grant(255).unwrap(),
                    root.native_grant(255).unwrap(),
                )],
            )
            .unwrap();
        let (healthy_actor, mut actor_peer) = retry::healthy_actor(&calls, &tasks, &wire).await;
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(1),
        };
        let grant = || {
            calls.register(
                &wire,
                binding.clone(),
                CarrierContext {
                    session: None,
                    deadline: None,
                    cancel: CancellationToken::new(),
                },
                0,
                Arc::from([]),
                Arc::from([]),
            )
        };
        let first = grant().unwrap();
        let custody = first
            .pending_observer()
            .packet_driver_custody(&first.identity(), &first.call())
            .unwrap()
            .unwrap();
        let users = crate::runtime::packet_flow::FlowUsers::new(NonZeroUsize::MIN);
        let user = users
            .acquire_from_packet_call(wire.clone(), Some(custody))
            .unwrap();
        let receiver = user.receiver_binding();
        let session = session(
            socket,
            peer,
            target.clone(),
            AssociationId(1),
            CancellationToken::new(),
            user,
            users.cancellation().clone(),
        );
        let mut loan = DatagramLoan::new(
            zero_platform_tokio::contract::datagram::erase_session(session),
            original.clone(),
            AssociationId(1),
            target.clone(),
        );
        let observation = loan.receiver_observation();
        receiver.bind(observation, &mut loan.owner).unwrap();
        let (session, handoff) = first.install_datagram_loan(loan, AssociationId(1)).unwrap();
        handoff.confirm_receiver(&wire).unwrap();
        let zero_platform_tokio::contract::datagram::DatagramSession {
            tx,
            rx,
            mut control,
            ..
        } = session;
        drop(tx);
        drop(rx);
        let first = Arc::new(
            control
                .wait_closed(tokio::time::Instant::now() + Duration::from_millis(10))
                .await
                .unwrap_err(),
        );
        assert_eq!(first.reason, Reason::Timeout);
        assert!(
            grant().is_err(),
            "failed original continuation still occupies N=1"
        );
        let retained = zero_platform_tokio::contract::lifecycle::ResourceCleanupFailures::default();
        retained.receive(
            first.clone(),
            zero_platform_tokio::contract::lifecycle::CleanupFailurePhase::Driver,
        );
        let receipt = first.cleanup_failure_receipt().unwrap();
        assert!(receipt.is_acknowledged());
        assert!(
            grant().is_err(),
            "fault receipt ACK cannot refund unfinished real IO"
        );
        controller.write_all(&[1]).await.unwrap();
        drained.await.unwrap(); // actual sender future returned, not Source join
        assert!(!wire.is_joined());
        assert!(control.try_close().unwrap().is_complete());
        calls.collect_retired();
        let next = grant().unwrap(); // same Calls, Source Scope and Kernel Root
        assert!(
            !healthy_actor.is_joined(),
            "healthy long-lived actor does not self-join for an attempt"
        );
        assert_eq!(total.snapshot().charged, 1);
        assert!(!wire.is_joined());
        assert_eq!(first.reason, Reason::Timeout); // original deadline retained
        assert!(retained
            .receipt_failure()
            .unwrap()
            .cleanup_failure_receipt()
            .unwrap()
            .same_receipt(receipt));
        let queued = output.recv().await.unwrap();
        assert_eq!(
            zero_stack::packet::parse_udp(&queued).unwrap().payload,
            b"first"
        );
        drop(queued);
        retry::actual_retry(
            next,
            &wire,
            &users,
            &stack,
            &read,
            &original,
            peer,
            target,
            &mut output,
        )
        .await;
        calls.collect_retired();
        assert!(calls
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .is_complete());
        assert_eq!(root.transmit().occupied.frames, 0);
        scope.begin_shutdown();
        controller.write_all(&[2]).await.unwrap();
        let exit = scope.next_receipt().await.unwrap();
        assert!(wire.matches_joined(exit.joined()));
        assert_eq!(total.snapshot().charged, 1);
        let delayed = scope.defer_callbacks();
        scope.acknowledge(&exit).unwrap();
        assert!(!wire.try_joined().unwrap().registration_released());
        assert_eq!(total.snapshot().charged, 1);
        drop(delayed);
        wire.wait_registration_released().await;
        assert_eq!(
            total.snapshot().charged,
            1,
            "whole Source still owns its actual healthy worker"
        );
        actor_peer.write_all(&[1]).await.unwrap();
        let actor_exit = scope.next_receipt().await.unwrap();
        assert!(healthy_actor.matches_joined(actor_exit.joined()));
        assert_eq!(total.snapshot().charged, 1);
        scope.acknowledge(&actor_exit).unwrap();
        assert_eq!(total.snapshot().charged, 0);
        assert!(original.closed().is_none());
        scope.begin_shutdown();
        assert!(scope.is_drained());
        drop(control);
        drop(handoff);
        drop(calls);
        drop(read);
        drop(stack);
        assert_eq!(
            root.native().occupied.frames,
            0,
            "actual workspace custody has returned"
        );
    })
    .await
    .unwrap();
}
