//! Stable child fault delivery cannot sign the real accepted TCP/task closed.
use super::*;
use crate::{
    contract::{
        component::{AcceptedTcpLimits, ResourceDeclaration},
        lifecycle::{erase_lifecycle, ResourceLifecycle},
        scope::{TaskClass, TaskScope},
        CleanupFailureReceipt, RetainedError,
    },
    ControlledTcpListenerOwner,
};
use std::time::Duration;
struct StableChild(RetainedError);
impl ResourceLifecycle for StableChild {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: crate::contract::CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        if !self.0.cleanup_failure_receipt().unwrap().is_acknowledged() {
            Err(ChannelError::retained(self.0.clone()))
        } else {
            Ok(CloseReport::default())
        }
    }
}
#[tokio::test]
async fn accepted_child_keeps_exact_cleanup_source_before_ack_then_closes_separately_from_real_tcp_and_host_receipt(
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let one = NonZeroUsize::MIN;
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        let declaration = ResourceDeclaration::TcpListener {
            destination: zero_core::request::Destination::new(
                zero_core::Address::Ipv4([127, 0, 0, 1]),
                0,
            ),
            max_bind_attempts: one,
            accepted: Some(AcceptedTcpLimits {
                max_sockets: one,
                max_tasks_per_socket: one,
                max_resources_per_socket: 2,
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
        scope.enable_registration().unwrap();
        let (peer, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept_owned()
        );
        let peer = peer.unwrap();
        let (accepted, _) = accepted.unwrap();
        let claim = accepted.claim();
        let bad = tokio::net::TcpSocket::new_v4().unwrap();
        bad.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let actual = Arc::new(
            tokio::net::TcpStream::connect(bad.local_addr().unwrap())
                .await
                .unwrap_err(),
        );
        // This is an actual foreign/library source; no paid Host header claim.
        let receipt = CleanupFailureReceipt::new();
        assert!(receipt.bind_payload(actual.clone()));
        let source = RetainedError::from_library(Arc::new(
            ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                .with_source(actual)
                .with_cleanup_failure_receipt(receipt.clone()),
        ));
        let child = source.clone();
        let (ready, receive) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let receiver = owner.accepted_task_receiver().unwrap();
        let (id, witness) = accepted
            .spawn_handler_bound(TaskClass::Auxiliary, move |io, tasks| async move {
                let caller = tasks.caller()?;
                caller.retain_resource(1, erase_lifecycle(StableChild(child)))?;
                ready
                    .send(caller)
                    .unwrap_or_else(|_| panic!("same original accepted caller"));
                let _ = released.await;
                drop(io);
                Ok(())
            })
            .unwrap();
        let caller = receive.await.unwrap();
        let users = caller.users.upgrade().unwrap();
        let row = users
            .state()
            .resources
            .iter()
            .find(|row| row.ordinal == 1)
            .unwrap()
            .clone();
        let deadline = Instant::now() + Duration::from_secs(3);
        assert!(!users.close_resource(&row, deadline).await);
        assert!(row.cleanup_delivery.get().unwrap().ptr_eq(&source));
        assert!(receipt.is_acknowledged());
        assert!(!witness.is_joined());
        assert_eq!(
            owner.accepted_evidence().unwrap().confirmed_native_closes,
            0
        );
        assert_eq!(
            peer.try_read(&mut [0]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(users.close_resource(&row, deadline).await);
        assert!(row.owner.lock().await.is_none());
        assert!(
            row.cleanup_delivery.get().unwrap().ptr_eq(&source),
            "same source survives actual child Box deallocation"
        );
        release.send(()).unwrap();
        let joined = scope.next_receipt().await.unwrap();
        assert_eq!(joined.id(), id);
        assert!(receiver.observe_joined(id).unwrap().result().is_ok());
        assert!(!joined.joined().registration_released());
        assert_eq!(
            owner.accepted_evidence().unwrap().confirmed_native_closes,
            0
        );
        scope.acknowledge(&joined).unwrap();
        let closed = owner
            .close_accepted_graph(&claim, deadline)
            .await
            .unwrap()
            .unwrap();
        assert!(closed.matches(&claim));
        assert_eq!(
            owner.accepted_evidence().unwrap().confirmed_native_closes,
            1
        );
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
