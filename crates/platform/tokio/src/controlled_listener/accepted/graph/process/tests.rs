use super::*;
use crate::{
    contract::{
        component::{AcceptedTcpLimits, ResourceDeclaration},
        lifecycle::TaskClass,
        scope::TaskScope,
    },
    ControlledTcpListenerOwner,
};
use tokio::{io::AsyncReadExt, net::TcpStream};
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[tokio::test]
async fn accepted_metadata_uses_real_same_entry_helper_and_keeps_original_until_native_join_and_ack(
) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let one = NonZeroUsize::MIN;
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(2));
        let declaration = ResourceDeclaration::TcpListener {
            destination: zero_core::request::Destination::new(
                zero_core::Address::Ipv4([127, 0, 0, 1]),
                0,
            ),
            max_bind_attempts: one,
            accepted: Some(AcceptedTcpLimits {
                max_sockets: one,
                max_tasks_per_socket: one,
                max_resources_per_socket: 0,
            }),
        };
        let mut owner = ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration,
            one,
            CancellationToken::new(),
            one,
            one,
            &scope.registrar(),
        )
        .unwrap();
        let mut listener = owner.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
        let endpoint = listener.local_addr().unwrap();
        let receiver = owner.accepted_task_receiver().unwrap();
        scope.enable_registration().unwrap();
        let (peer, accepted) = tokio::join!(TcpStream::connect(endpoint), listener.accept_owned());
        let peer = peer.unwrap();
        let source = peer.local_addr().unwrap();
        let (accepted, actual_source) = accepted.unwrap();
        assert_eq!(source, actual_source);
        let claim = accepted.claim();
        let (sent, ready) = tokio::sync::oneshot::channel();
        let (id, caller_witness) = accepted
            .spawn_handler_bound(TaskClass::Auxiliary, move |mut io, tasks| async move {
                let caller = tasks.caller()?;
                let process = caller
                    .lookup_local_process(
                        source,
                        LocalProcessTransport::Tcp,
                        NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
                    )
                    .await?
                    .unwrap();
                assert_eq!(process.pid, std::process::id());
                sent.send(caller).ok().unwrap();
                assert!(io
                    .read_u8()
                    .await
                    .is_err_and(|source| source.kind() == std::io::ErrorKind::UnexpectedEof));
                Ok(())
            })
            .unwrap();
        let caller = ready.await.unwrap();
        let users = caller.users.upgrade().unwrap();
        let original = users.process_lookups().pop().unwrap();
        assert!(original.caller().same_registration(&caller_witness));
        let worker = original.witness().unwrap();
        assert_ne!(worker.task_id(), Some(id));
        let receipt = scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), worker.task_id().unwrap());
        assert!(receiver
            .observe_joined(receipt.id())
            .unwrap()
            .result()
            .is_ok());
        assert!(!original.retired());
        assert_eq!(
            users.process_lookups().len(),
            1,
            "same actual Node remains through hosted ACK"
        );
        let denied = caller
            .lookup_local_process(
                source,
                LocalProcessTransport::Tcp,
                NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
            )
            .await
            .unwrap_err();
        assert_eq!(denied.reason, Reason::Capacity);
        assert_eq!(
            denied.delivery,
            crate::contract::DeliveryState::NotSubmitted
        );
        assert!(
            caller.check_open().is_ok(),
            "only the new metadata job is denied"
        );
        scope.acknowledge(&receipt).unwrap();
        users.reap_process_lookups();
        assert!(original.retired());
        assert!(users.process_lookups().is_empty());
        // This is the actual same entry and registered blocking worker, with
        // a smaller explicit read budget. Failure cannot become missing PID
        // metadata and silently bypass the caller's process routing rules.
        let denied = caller
            .lookup_local_process(source, LocalProcessTransport::Tcp, NonZeroUsize::MIN)
            .await
            .unwrap_err();
        assert_eq!(denied.reason, Reason::Capacity);
        let receipt = scope.next_receipt().await.unwrap();
        let actual = receiver.observe_joined(receipt.id()).unwrap();
        assert!(actual.result().is_err());
        assert!(actual.failure_accepted());
        scope.acknowledge(&receipt).unwrap();
        assert!(caller.check_open().is_ok());
        let info = caller
            .lookup_local_process(
                source,
                LocalProcessTransport::Tcp,
                NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(info.pid, std::process::id());
        let receipt = scope.next_receipt().await.unwrap();
        assert!(receiver
            .observe_joined(receipt.id())
            .unwrap()
            .result()
            .is_ok());
        scope.acknowledge(&receipt).unwrap();
        drop(peer);
        let receipt = scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), id);
        assert!(receiver.observe_joined(id).unwrap().result().is_ok());
        scope.acknowledge(&receipt).unwrap();
        let end = Instant::now() + std::time::Duration::from_secs(2);
        let closed = owner
            .close_accepted_graph(&claim, end)
            .await
            .unwrap()
            .unwrap();
        assert!(closed.matches(&claim));
        assert!(owner.wait_closed(end).await.unwrap().is_complete());
        assert!(owner.accepted_evidence().unwrap().provider_confirmed());
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
