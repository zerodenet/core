use super::*;
use crate::{
    contract::{
        component::{AcceptedTcpLimits, ResourceDeclaration},
        lifecycle::{ChannelControl, TaskClass},
        scope::TaskScope,
        CancellationToken,
    },
    controlled_carrier::CarrierOriginalTotals,
    ControlledTcpListener, ControlledTcpListenerOwner,
};
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
    sync::Arc,
    task::Poll,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{timeout, Duration, Instant},
};
use zero_core::{
    component::{ComponentId, ResourceGeneration},
    request::Destination,
    Address,
};
use zero_tun::packet_memory::PacketReturnTotals;
mod library;
mod pressure;
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(503),
        generation: ResourceGeneration(1),
    }
}
fn declaration() -> ResourceDeclaration {
    ResourceDeclaration::TcpListener {
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: NonZeroUsize::MIN,
        accepted: Some(AcceptedTcpLimits {
            max_sockets: NonZeroUsize::MIN,
            max_tasks_per_socket: NonZeroUsize::MIN,
            max_resources_per_socket: 0,
        }),
    }
}
fn metadata_limit(rows: &[usize]) -> PacketMetadataLayout {
    let mut total = zero_traits::packet_metadata::PacketMetadataCounter::owned_layout()
        .checked_add(PacketMetadataArc::<Original>::layout().unwrap())
        .unwrap();
    let headers = liability::HeaderLiability::layout().unwrap();
    for _ in 0..rows.iter().copied().max().unwrap_or(1) {
        total = total.checked_add(headers).unwrap();
    }
    for &capacity in rows {
        total = total
            .checked_add(PacketMetadataLayout::array::<rows::Cell>(capacity).unwrap())
            .unwrap();
    }
    total
}
fn packet_root(limit: PacketMetadataLayout) -> Arc<PacketReturnTotals> {
    let one = NonZeroUsize::MIN;
    Arc::new(PacketReturnTotals::new_with_metadata(one, one, one, limit).unwrap())
}
fn listener(scope: &TaskScope) -> ControlledTcpListenerOwner {
    ControlledTcpListenerOwner::new_with_accepted_graph(
        &declaration(),
        NonZeroUsize::MIN,
        CancellationToken::new(),
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        &scope.registrar(),
    )
    .unwrap()
}
async fn connection(
    listener: &mut ControlledTcpListener,
) -> (TcpStream, super::super::AcceptedTcp) {
    let address = listener.local_addr().unwrap();
    let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept_owned());
    (client.unwrap(), accepted.unwrap().0)
}
async fn reject(
    scope: &mut TaskScope,
    owner: &mut ControlledTcpListenerOwner,
    listener: &mut ControlledTcpListener,
) -> TaskJoined {
    let receiver = owner.accepted_task_receiver().unwrap();
    let (mut peer, accepted) = connection(listener).await;
    let claim = accepted.claim();
    let (id, witness) = accepted
        .spawn_handler_bound(TaskClass::Auxiliary, |mut original, _| async move {
            let mut byte = [0];
            let source = original.read_exact(&mut byte).await.unwrap_err();
            Err(ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source))
        })
        .unwrap();
    peer.shutdown().await.unwrap();
    let exit = scope.next_exit().await.unwrap();
    assert_eq!(exit.id, id);
    let joined = witness.wait().await;
    assert!(joined.result().is_err());
    assert!(joined.registration_released());
    assert!(!joined.failure_accepted());
    let actual = receiver.observe_joined(id).unwrap();
    assert!(actual.failure_accepted());
    assert!(Arc::ptr_eq(
        &actual.result().unwrap_err(),
        &joined.result().unwrap_err()
    ));
    let closed = owner
        .close_accepted_graph(&claim, deadline())
        .await
        .unwrap()
        .unwrap();
    assert!(closed.matches(&claim));
    drop(peer);
    joined
}
async fn pending(future: impl Future) {
    tokio::pin!(future);
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

fn rejected(
    result: Result<(super::super::AcceptedTcp, std::net::SocketAddr), ChannelError>,
) -> ChannelError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("native accept passed exhausted liability capacity"),
    }
}
