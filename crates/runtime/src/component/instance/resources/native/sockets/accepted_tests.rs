//! Actual ordinary Host construction, not a custom ResourceAccess substitute.
use super::*;
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
    task::Poll,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::component::{ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::contract::{
    component::AcceptedTcpLimits,
    scope::{TaskClass, TaskScope},
};
fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn end() -> Instant {
    Instant::now() + std::time::Duration::from_secs(3)
}
#[tokio::test]
async fn ordinary_host_accepted_source_uses_same_roots_real_receiver_and_global_host_ack() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let roots = Arc::new(crate::HostOriginalBudgets::default());
        roots.carrier.install_after_retirement(nz(2)).unwrap();
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(nz(1)));
        let instance = ComponentInstanceId {
            component: ComponentId(888),
            generation: ResourceGeneration(1),
        };
        let destination = Destination::new(Address::Ipv4([127, 0, 0, 1]), 0);
        let declaration = ResourceDeclaration::TcpListener {
            destination: destination.clone(),
            max_bind_attempts: nz(1),
            accepted: Some(AcceptedTcpLimits {
                max_sockets: nz(1),
                max_tasks_per_socket: nz(1),
                max_resources_per_socket: 0,
            }),
        };
        let baseline = roots.packet.driver_metadata_view().snapshot();
        let resources = HostResources::new_hosted(
            instance,
            &[declaration.clone()],
            CancellationToken::new(),
            scope.registrar().downgrade(),
            roots.clone(),
        )
        .unwrap();
        assert_eq!(
            roots.carrier.snapshot().charged,
            0,
            "cold graph binding opens no native"
        );
        assert_ne!(
            roots.packet.driver_metadata_view().snapshot(),
            baseline,
            "the actual Source failure receiver is paid before native"
        );
        let receiver = resources.original_accepted_receiver(&destination).unwrap();
        assert!(receiver.task_failures().unwrap().matches(
            instance,
            &scope.registrar().scope_identity(),
            &roots.packet.driver_metadata_view()
        ));
        let access = resources.activate(end()).unwrap();
        scope.enable_registration().unwrap();
        let mut listener = resources.listen_tcp(destination.clone()).await.unwrap();
        assert_eq!(roots.carrier.snapshot().charged, 1);
        let (client, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept_owned()
        );
        let mut client = client.unwrap();
        let (accepted, _) = accepted.unwrap();
        let claim = accepted.claim();
        let origin = receiver.carrier_origin(&claim).unwrap();
        assert!(origin.matches_receiver(&receiver));
        assert!(!origin.is_settled());
        assert_eq!(roots.carrier.snapshot().charged, 2);
        // Equal limits/root/Scope still cannot name another actual provider.
        let other = HostResources::new_hosted(
            ComponentInstanceId {
                component: ComponentId(889),
                generation: ResourceGeneration(1),
            },
            &[declaration],
            CancellationToken::new(),
            scope.registrar().downgrade(),
            roots.clone(),
        )
        .unwrap();
        let foreign = other.original_accepted_receiver(&destination).unwrap();
        assert!(!origin.matches_receiver(&foreign));
        assert_eq!(
            foreign.carrier_origin(&claim).err().unwrap().reason,
            Reason::StalePlan
        );
        let (waiting, ready) = tokio::sync::oneshot::channel();
        let (id, witness) = accepted
            .spawn_handler(
                &scope.registrar(),
                TaskClass::Auxiliary,
                move |mut io, _| async move {
                    let mut bytes = [0; 4];
                    let mut first = Box::pin(io.read_exact(&mut bytes));
                    poll_fn(|cx| {
                        assert!(first.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    waiting.send(()).unwrap();
                    first.as_mut().await.map_err(|source| {
                        error(ComponentId(888), Reason::Io, "accepted read").with_source(source)
                    })?;
                    drop(first);
                    io.write_all(&bytes).await.map_err(|source| {
                        error(ComponentId(888), Reason::Io, "accepted write").with_source(source)
                    })?;
                    drop(io);
                    Ok(())
                },
            )
            .unwrap();
        ready.await.unwrap();
        client.write_all(b"same").await.unwrap();
        let mut bytes = [0; 4];
        client.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"same");
        let receipt = scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), id);
        assert!(witness.matches_joined(receipt.joined()));
        assert!(receiver
            .observe_joined_for_claim(&claim, id)
            .unwrap()
            .result()
            .is_ok());
        let native = receiver.close(&claim, end()).await.unwrap().unwrap();
        assert!(native.matches(&claim));
        assert!(origin.native_closed().unwrap().matches(&claim));
        assert!(!origin.is_settled());
        assert!(!witness.try_joined().unwrap().registration_released());
        assert_eq!(roots.carrier.snapshot().charged, 2);
        scope.acknowledge(&receipt).unwrap();
        origin.wait_settled_original().await.unwrap();
        assert!(origin.closed().unwrap().matches(&claim));
        assert_eq!(roots.carrier.snapshot().charged, 1);
        assert_eq!(client.read(&mut bytes).await.unwrap(), 0);
        drop(access);
        drop(listener);
        assert!(resources.shutdown_io(end()).await.unwrap().is_complete());
        assert!(other.shutdown_io(end()).await.unwrap().is_complete());
        assert_eq!(roots.carrier.snapshot().charged, 0);
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
