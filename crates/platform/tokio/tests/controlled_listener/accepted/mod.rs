use super::*;
use crate::contract::{component::ResourceDeclaration, lifecycle::Unfinished};
use std::{
    future::Future,
    task::{Context, Waker},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use zero_core::{request::Destination, Address};
mod facade;
mod failures;
mod graph;
mod wake;
fn one() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn declaration() -> ResourceDeclaration {
    ResourceDeclaration::TcpListener {
        accepted: None,
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: one(),
    }
}
fn bound() -> (
    ControlledTcpListenerOwner,
    ControlledTcpListener,
    SocketAddr,
) {
    let mut owner = ControlledTcpListenerOwner::new_with_accepted_limit(
        &declaration(),
        one(),
        CancellationToken::new(),
        one(),
    )
    .unwrap();
    let view = owner.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = view.local_addr().unwrap();
    (owner, view, address)
}
async fn pair(
    view: &mut ControlledTcpListener,
    address: SocketAddr,
) -> (TcpStream, AcceptedTcpStream, AcceptedTcpClaim) {
    let (peer, accepted) = tokio::join!(TcpStream::connect(address), view.accept_owned());
    let (accepted, remote) = accepted.unwrap();
    let peer = peer.unwrap();
    assert_eq!(remote, peer.local_addr().unwrap());
    let (io, claim) = accepted.into_parts();
    (peer, io, claim)
}
#[tokio::test]
async fn pending_cancel_releases_only_unused_reservation_and_legacy_accept_cannot_bypass_cap() {
    let (mut owner, mut view, address) = bound();
    assert_eq!(
        view.accept().await.err().unwrap().kind(),
        io::ErrorKind::Unsupported
    );
    {
        let mut pending = Box::pin(view.accept_owned());
        assert!(pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        let evidence = owner.accepted_evidence().unwrap();
        assert_eq!(evidence.retained, 1);
        assert_eq!(evidence.confirmed_native_closes, 0);
        assert!(!evidence.provider_confirmed());
    }
    assert_eq!(owner.accepted_evidence().unwrap().retained, 0);
    let (mut peer, mut io, claim) = pair(&mut view, address).await;
    io.write_all(b"actual").await.unwrap();
    let mut bytes = [0; 6];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"actual");
    assert!(owner
        .close_accepted(&claim, deadline())
        .await
        .unwrap()
        .unwrap()
        .matches(&claim));
    assert_eq!(peer.read(&mut bytes).await.unwrap(), 0);
    let report = owner.wait_closed(deadline()).await.unwrap();
    assert!(
        !report.is_complete(),
        "transferred users/tasks remain Unaccounted"
    );
    assert!(report
        .unfinished
        .iter()
        .any(|item| matches!(item, Unfinished::Resource(_))));
    assert_eq!(
        owner.accepted_evidence().unwrap().users,
        AcceptedTcpUsers::Unaccounted
    );
    assert!(!owner.accepted_evidence().unwrap().provider_confirmed());
}
#[tokio::test]
async fn actual_child_close_preserves_same_entry_receipt_foreign_isolation_and_capacity() {
    let (mut first, mut first_view, address) = bound();
    let (mut second, mut second_view, second_address) = bound();
    let (mut peer, mut io, claim) = pair(&mut first_view, address).await;
    let (_other_peer, other_io, other) = pair(&mut second_view, second_address).await;
    assert_eq!(claim.id(), other.id());
    assert!(second.close_accepted(&claim, deadline()).await.is_err());
    assert!(
        first_view.accept_owned().await.is_err(),
        "full cap refuses before native accept"
    );
    assert!(io.local_addr().is_ok() && other_io.local_addr().is_ok());
    peer.write_all(b"x").await.unwrap();
    let mut byte = [0];
    io.read_exact(&mut byte).await.unwrap();
    assert_eq!(byte, [b'x']);
    assert!(first
        .close_accepted(&claim, Instant::now())
        .await
        .unwrap()
        .is_none());
    assert_eq!(first.accepted_evidence().unwrap().retained, 1);
    // wait_closed revokes I/O even when its deadline has already elapsed;
    // unconfirmed native ownership does not promise the view remains writable.
    assert_eq!(
        io.read(&mut byte).await.unwrap_err().kind(),
        io::ErrorKind::ConnectionAborted
    );
    assert_eq!(
        peer.try_read(&mut byte).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(first.take_accepted_report().closed_io.is_empty());
    let receipt = first
        .close_accepted(&claim, deadline())
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.matches(&claim));
    assert!(!receipt.matches(&other));
    assert_eq!(first.take_accepted_report().closed_io.len(), 1);
    assert!(first
        .close_accepted(&claim, deadline())
        .await
        .unwrap()
        .unwrap()
        .matches(&claim));
    assert!(first.take_accepted_report().closed_io.is_empty());
    let (_replacement_peer, replacement, replacement_claim) = pair(&mut first_view, address).await;
    assert_ne!(replacement_claim.id(), claim.id());
    first.close_accepted(&claim, deadline()).await.unwrap();
    assert!(replacement.local_addr().is_ok());
    first
        .close_accepted(&replacement_claim, deadline())
        .await
        .unwrap();
    second.close_accepted(&other, deadline()).await.unwrap();
    first.wait_closed(deadline()).await.unwrap();
    second.wait_closed(deadline()).await.unwrap();
}
#[tokio::test]
async fn sixty_five_actual_accepts_reuse_one_native_slot_without_claiming_user_joins() {
    timeout(Duration::from_secs(10), async {
        let (mut owner, mut view, address) = bound();
        for sequence in 0u8..65 {
            let (mut peer, mut io, claim) = pair(&mut view, address).await;
            peer.write_all(&[sequence]).await.unwrap();
            let mut byte = [0];
            io.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte, [sequence]);
            io.write_all(&byte).await.unwrap();
            peer.read_exact(&mut byte).await.unwrap();
            drop(io);
            assert_eq!(
                owner.accepted_evidence().unwrap().retained,
                1,
                "view Drop is no native receipt"
            );
            assert!(owner
                .close_accepted(&claim, deadline())
                .await
                .unwrap()
                .unwrap()
                .matches(&claim));
            assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
        }
        let evidence = owner.accepted_evidence().unwrap();
        assert_eq!(evidence.confirmed_native_closes, 65);
        assert_eq!(evidence.retained, 0);
        assert_eq!(evidence.users, AcceptedTcpUsers::Unaccounted);
        assert!(!owner.wait_closed(deadline()).await.unwrap().is_complete());
        assert!(owner.take_native_report().closed_io.is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retained_split_users_never_turn_native_receipt_into_whole_provider_confirmation() {
    let (mut owner, mut view, address) = bound();
    let (_peer, io, claim) = pair(&mut view, address).await;
    let (mut reader, mut writer) = tokio::io::split(io);
    let mut bytes = [0; 8];
    let mut pending = Box::pin(reader.read(&mut bytes));
    assert!(pending
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    let receipt = owner
        .close_accepted(&claim, deadline())
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.matches(&claim));
    assert!(pending.await.is_err());
    assert!(writer.write_all(b"closed").await.is_err());
    let report = owner.wait_closed(deadline()).await.unwrap();
    assert!(!report.is_complete());
    assert!(!owner.accepted_evidence().unwrap().provider_confirmed());
    drop((reader, writer));
    assert!(
        !owner.wait_closed(deadline()).await.unwrap().is_complete(),
        "split Drop is not a task/user join witness"
    );
}
