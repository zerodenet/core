//! Real accepted Source for low-level UDP tests, including its original close.
use super::UdpIngressRuntime;
use crate::Proxy;
use std::{future::Future, num::NonZeroUsize, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
    time::Instant,
};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::{
    contract::{
        component::{AcceptedTcpLimits, ResourceDeclaration},
        scope::{TaskClass, TaskScope},
        CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    AcceptedTaskFailures, ControlledTcpListenerOwner,
};

#[path = "source/dispatch.rs"]
mod dispatch;
pub(crate) use dispatch::Source;

fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
}
fn end() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

pub(crate) async fn run<F, Fut, T>(proxy: &Proxy, work: F) -> T
where
    F: FnOnce(Proxy, Source) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let one = NonZeroUsize::MIN;
    let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(one));
    let declaration = ResourceDeclaration::TcpListener {
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
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
    let instance = proxy.protocols.allocate_resource_identity().unwrap();
    let roots = proxy.protocols.runtime_host().original_budgets();
    scope
        .bind_task_budget(roots.component_tasks.clone())
        .unwrap();
    let failures = AcceptedTaskFailures::prepare(
        instance,
        &scope.registrar(),
        &roots.packet.driver_metadata_view(),
    )
    .unwrap();
    owner.bind_task_failure_receiver(&failures).unwrap();
    owner
        .bind_original_totals(instance, &scope.registrar(), &roots.carrier)
        .unwrap();
    let mut listener = owner.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
    scope.enable_registration().unwrap();
    let (peer, accepted) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept_owned()
    );
    let mut peer = peer.unwrap();
    let (accepted, remote) = accepted.unwrap();
    assert_eq!(peer.local_addr().unwrap(), remote);
    let claim = accepted.claim();
    let (send, receive) = oneshot::channel();
    let actual = proxy.clone();
    let (_, witness) = accepted
        .spawn_handler(
            &scope.registrar(),
            TaskClass::Auxiliary,
            move |mut socket, tasks| async move {
                let mut hello = [0; 2];
                socket.read_exact(&mut hello).await.map_err(failure)?;
                assert_eq!(&hello, b"go");
                let caller = tasks.caller()?;
                let stopped = CancellationToken::new();
                let (original, state) = dispatch::Owner::new(&actual, stopped.clone());
                let retained = caller.retain_logical_resource_with_driver(
                    zero_platform_tokio::contract::lifecycle::erase_lifecycle(original),
                    &tasks,
                )?;
                let (slot, job) =
                    crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot::channel();
                let runtime = UdpIngressRuntime::new(
                    actual
                        .tcp_runtime_services()
                        .for_inbound("incoming".into())
                        .with_inbound_instance(instance),
                )
                .with_accepted_caller(caller)
                .with_datagram_returns(dispatch::Owner::returns(&state))
                .with_inline_direct_driver(slot, stopped);
                let runtime = Source::new(runtime, state);
                let consumer = async move {
                    let result = work(actual, runtime).await;
                    assert!(send.send(result).is_ok(), "actual UDP Source receiver");
                    Ok(())
                };
                crate::runtime::udp_socket::endpoint::peer::job::run_peer_preserving(consumer, job)
                    .await
                    .map_err(failure)?;
                drop(retained);
                drop(socket);
                Ok(())
            },
        )
        .unwrap();
    peer.write_all(b"go").await.unwrap();
    let result = proxy
        .protocols
        .drive_component_supervision(receive)
        .await
        .unwrap();
    proxy
        .protocols
        .drive_component_supervision(async {
            let exit = scope.next_exit().await.unwrap();
            let joined = owner
                .accepted_task_receiver()
                .unwrap()
                .observe_joined(exit.id)
                .unwrap();
            assert!(joined.result().is_ok());
            assert!(witness.matches_joined(&witness.try_joined().unwrap()));
            assert!(exit.result.is_ok());
            assert_eq!(
                peer.try_read(&mut [0; 1]).unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
            assert!(owner
                .close_accepted_graph(&claim, end())
                .await
                .unwrap()
                .unwrap()
                .matches(&claim));
            assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
            drop(listener);
            assert!(owner.wait_closed(end()).await.unwrap().is_complete());
            assert!(scope.shutdown(end()).await.is_complete());
            assert!(owner.accepted_evidence().unwrap().provider_confirmed());
        })
        .await;
    result
}
