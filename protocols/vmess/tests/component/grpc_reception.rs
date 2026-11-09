//! The actual later TCP failure must be received while first setup is pending.
use super::{state::Record, *};
use std::{future::Future, num::NonZeroUsize, sync::atomic::AtomicBool, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{component::*, request::Destination, Address};
use zero_platform_tokio::{
    contract::{
        lifecycle::ResourceLifecycle,
        network::{CarrierContext, UpstreamBinding, UpstreamTransport},
        scope::{SpawnError, TaskClass, TaskJoinWitness, TaskRegistrar, TaskScope},
        CancellationToken,
    },
    controlled_carrier::{CarrierDriver, CarrierIo, CarrierOwner, NativeCarrierOpener},
    EgressInterfaceControl,
};

fn record(grant: &zero_platform_tokio::controlled_carrier::CarrierGrant) -> Arc<Record> {
    let adoption = grant.adoption();
    Arc::new(Record {
        stop: grant.cancellation(),
        adoption,
        capacity: Mutex::new(None),
        witness: Mutex::new(None),
        exit_changed: tokio::sync::Notify::new(),
        received: Mutex::new(None),
        accepted: AtomicBool::new(false),
        opening: AtomicBool::new(true),
        published: AtomicBool::new(false),
        done: AtomicBool::new(false),
        user_live: AtomicBool::new(false),
        tracked_view: false,
    })
}
fn install(
    record: &Arc<Record>,
    tasks: &TaskRegistrar,
    io: CarrierDriver<CarrierIo>,
) -> TaskJoinWitness {
    let (_, witness) = record
        .adoption
        .spawn_driver(tasks, TaskClass::Auxiliary, io, |mut io| async move {
            let mut byte = [0];
            let source = io.read_exact(&mut byte).await.unwrap_err();
            Err(
                ChannelError::new(Stage::Receive, Reason::Io, Impact::Component)
                    .with_source(source),
            )
        })
        .unwrap();
    record.install_witness(witness.clone());
    record
        .opening
        .store(false, std::sync::atomic::Ordering::Release);
    record.exit_changed.notify_waiters();
    witness
}

#[tokio::test]
async fn late_first_original_does_not_block_receiving_the_second_real_tcp_failure() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let deadline = Instant::now() + Duration::from_secs(5);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let actual = ComponentInstanceId {
            component: ComponentId(914),
            generation: ResourceGeneration(1),
        };
        let egress = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let upstream = UpstreamTransport::new(
            binding.clone(),
            Arc::new(NativeCarrierOpener::new(actual, binding, peer, egress, None).unwrap()),
        );
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(2));
        scope.enable_registration().unwrap();
        // Explicit standalone fixture owner; both are real original sockets,
        // actual same-owner drivers and the same original HostedScope ledger.
        let mut owner = CarrierOwner::new(
            actual,
            NonZeroUsize::new(2).unwrap(),
            &CancellationToken::new(),
        );
        owner.start(deadline).await.unwrap();
        let mut records = Vec::new();
        let mut drivers = Vec::new();
        let mut peers = Vec::new();
        for _ in 0..2 {
            let grant = owner.grants().reserve().unwrap();
            records.push(record(&grant));
            let io = upstream
                .acquire_controlled_stream(
                    Destination::new(Address::Ipv4([127, 0, 0, 1]), peer.port()),
                    CarrierContext {
                        session: None,
                        deadline: Some(deadline),
                        cancel: CancellationToken::new(),
                    },
                    grant,
                )
                .await
                .unwrap();
            drivers.push(io.into_driver());
            peers.push(listener.accept().await.unwrap().0);
        }
        let second = install(&records[1], &scope.registrar(), drivers.pop().unwrap());
        peers[1].shutdown().await.unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(second.matches_joined(receipt.joined()));
        let original = receipt.result().unwrap_err();
        assert!(!receipt.local_failure_accepted());
        let receive = super::reception::receive_all(records.clone(), deadline);
        tokio::pin!(receive);
        std::future::poll_fn(|cx| {
            assert!(
                receive.as_mut().poll(cx).is_pending(),
                "first setup is still pending"
            );
            if receipt.local_failure_accepted() {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
        assert!(!receipt.joined().registration_released());
        let retained = records[1].received.lock().unwrap().clone().unwrap();
        assert!(second.matches_joined(&retained));
        assert!(Arc::ptr_eq(&original, &retained.result().unwrap_err()));
        assert_eq!(owner.retained(), 2);
        let first = install(&records[0], &scope.registrar(), drivers.pop().unwrap());
        peers[0].shutdown().await.unwrap();
        receive.await;
        let first_receipt = scope.next_receipt().await.unwrap();
        assert!(first.matches_joined(first_receipt.joined()));
        assert!(first_receipt.result().is_err());
        assert!(first_receipt.local_failure_accepted());
        assert!(!first_receipt.joined().registration_released());
        assert_eq!(
            scope
                .registrar()
                .spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        );
        scope.acknowledge(&receipt).unwrap();
        scope.acknowledge(&first_receipt).unwrap();
        let report = owner.shutdown(deadline).await.unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert_eq!(report.closed_io.len(), 2);
        assert_eq!(owner.retained(), 0);
        assert!(records.iter().all(|record| record
            .adoption
            .closed()
            .is_some_and(|closed| closed.matches(&record.adoption))));
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
