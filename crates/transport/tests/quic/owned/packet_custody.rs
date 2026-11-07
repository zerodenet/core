//! Exact packet identity binds the live endpoint ledger; native closure does
//! not refund that original while the real endpoint Host receipt is held.
use super::*;
use crate::quic::owned::packet::{PacketSegment, PacketSocket};
use std::time::Duration;
use tokio::time::{timeout, Instant};
use zero_core::component::{
    ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration,
};
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration,
        scope::{TaskClass, TaskScope},
        CancellationToken,
    },
    controlled_carrier::CarrierOriginalTotals,
    controlled_datagram::metadata_pool::MetadataPoolOwner,
    EgressInterfaceControl,
};
#[tokio::test]
async fn same_packet_endpoint_registers_late_real_driver_and_keeps_native_n_until_its_actual_ack() {
    timeout(Duration::from_secs(3), async {
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(bound(3)));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let host = Arc::new(CancellationToken::new());
        let totals = CarrierOriginalTotals::new(bound(2));
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let actual = ComponentInstanceId {
            component: ComponentId(763),
            generation: ResourceGeneration(1),
        };
        let limits = MetadataLimits {
            main_sockets: bound(1),
            probe_sockets: bound(1),
            probe_attempts_per_initialization: bound(4),
            concurrent_send_calls: bound(1),
            write_pollers: bound(2),
        };
        let metadata = MetadataPoolOwner::new(actual, bound(1), limits, &host).unwrap();
        let registration = metadata.registration();
        registration
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledMetadataDatagram {
                    endpoint: remote,
                    dial: Default::default(),
                    limits,
                    max_origins: bound(1),
                },
                &tasks,
                &host,
                &totals,
            )
            .unwrap();
        metadata.start().unwrap();
        let grants = metadata.grants();
        let end = Instant::now() + Duration::from_secs(2);
        let (begin, started) = tokio::sync::oneshot::channel::<(
            zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolPermit,
            zero_platform_tokio::contract::scope::TaskJoinWitness,
        )>();
        let (publish, published) = tokio::sync::oneshot::channel();
        let (_, caller) = tasks
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let (permit, caller) = started.await.unwrap();
                let egress = EgressInterfaceControl::default();
                let grant = permit.open(
                    &caller,
                    &egress,
                    NetworkGeneration(egress.generation()),
                    end,
                )?;
                assert!(publish.send((permit, grant)).is_ok());
                Ok(())
            })
            .unwrap();
        let permit = grants.reserve(&caller).unwrap();
        let original = permit.origin();
        let buffers = totals.buffer_budget();
        let (socket, queues) =
            PacketSocket::prepare_with_budget(remote, buffers.clone(), &[]).unwrap();
        assert!(
            buffers.used_bytes() > 0,
            "same Root prepays before the original setup gate can open native IO"
        );
        assert!(begin.send((permit, caller.clone())).is_ok());
        let (permit, grant) = published.await.unwrap();
        let setup = scope.next_receipt().await.unwrap();
        assert!(caller.matches_joined(setup.joined()) && setup.result().is_ok());
        scope.acknowledge(&setup).unwrap();
        caller.wait_registration_released().await;
        let wire = socket.socket.clone();
        let mut segment = PacketSegment::prepare(grant, queues, tasks.clone(), remote, remote)
            .unwrap_or_else(|_| panic!("same actual native claim"));
        let (foreign_socket, _foreign_queues) = PacketSocket::prepare(remote).unwrap();
        let (foreign, _foreign_receiver) = OwnedEndpointOwner::new_packet(
            foreign_socket,
            tasks.clone(),
            OwnedEndpointLimits {
                drivers: bound(3),
                users: bound(1),
            },
        );
        assert_eq!(
            foreign.bind_packet_segment(&segment).unwrap_err().reason,
            zero_platform_tokio::contract::Reason::StalePlan,
            "same Scope is insufficient without the same actual queue"
        );
        drop(foreign);
        let (mut owner, mut receiver) = OwnedEndpointOwner::new_packet(
            socket,
            tasks.clone(),
            OwnedEndpointLimits {
                drivers: bound(3),
                users: bound(1),
            },
        );
        owner.bind_packet_segment(&segment).unwrap();
        assert!(
            owner.driver_tasks().is_empty(),
            "binding must observe future actual registrations, not freeze an empty list"
        );
        segment.start().unwrap();
        let _handle = owner.start(Default::default(), None, None).unwrap();
        let driver = owner.driver_tasks();
        assert_eq!(driver.len(), 1);
        wire.try_send(&quinn::udp::Transmit {
            destination: remote,
            ecn: None,
            contents: b"actual endpoint packet",
            segment_size: None,
            src_ip: None,
        })
        .unwrap();
        let mut bytes = [0; 64];
        let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], b"actual endpoint packet");
        assert_eq!(source.port(), original.endpoints().unwrap().0.port());
        assert_eq!(source.ip(), remote.ip());
        owner.begin_shutdown();
        let (endpoint, delivery) =
            tokio::join!(scope.next_receipt(), receiver.receive(driver[0], end));
        let endpoint = endpoint.unwrap();
        let delivery = delivery.unwrap();
        assert_eq!(endpoint.id(), driver[0]);
        assert!(endpoint.result().is_ok());
        assert_eq!(delivery.joined.id(), endpoint.id());
        receiver.release_body(&delivery).unwrap();
        assert!(owner
            .shutdown_users(end)
            .await
            .unwrap()
            .users_and_drivers_complete());
        let (closed, packet) = tokio::join!(segment.retire_final(end), scope.next_receipt());
        let (_queues, receipt) = closed.unwrap();
        let packet = packet.unwrap();
        assert!(packet.result().is_ok());
        assert_ne!(packet.id(), endpoint.id());
        assert_eq!(receipt.related_registrations().len(), 1);
        assert_eq!(
            receipt.related_registrations().next().unwrap().task_id(),
            Some(endpoint.id())
        );
        scope.acknowledge(&packet).unwrap();
        let closed = permit.close(Some(&receipt), &tasks).unwrap();
        assert!(closed.matches(&original) && closed.had_native_acquisition());
        assert!(!endpoint.joined().registration_released());
        assert_eq!(grants.retained(), Some(1));
        assert_eq!(totals.snapshot().charged, 1);
        assert!(!metadata.shutdown().unwrap().is_complete());
        assert!(registration.closed().is_none());
        scope.acknowledge(&endpoint).unwrap();
        assert!(metadata
            .shutdown_after_registration_ack(end)
            .await
            .unwrap()
            .is_complete());
        assert_eq!(grants.retained(), Some(0));
        assert_eq!(totals.snapshot().charged, 0);
        assert!(registration.closed().unwrap().matches(&registration));
        scope.begin_shutdown();
        assert!(scope.is_drained());
        assert!(
            buffers.used_bytes() > 0,
            "native N does not refund retained queue backing"
        );
        drop(_queues);
        drop(wire);
        drop(owner);
        drop(_handle);
        drop(segment);
        assert_eq!(buffers.used_bytes(), 0);
    })
    .await
    .unwrap();
}
