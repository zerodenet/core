use super::*;
#[path = "pool/driver.rs"]
mod driver;
use crate::component::test_support::*;
use std::{
    io,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn same_pool_cold_owner_declares_real_background_grant_and_no_default_budget() {
    let pool = Arc::new(ClientPool::default());
    let options = Arc::new(pool.component_options(NonZeroUsize::new(2).unwrap()));
    assert!(Weak::ptr_eq(&options.pool, &Arc::downgrade(&pool)));
    let spec = PoolFactory.describe(&options);
    assert!(spec.roles.is_empty());
    assert_eq!(spec.resources, vec![ResourceDeclaration::BackgroundCarrier]);
    assert_eq!(spec.task_limit, NonZeroUsize::new(2));
    let (_scope, mut no_grant) = context(2);
    no_grant.background_carrier = None;
    assert_eq!(
        PoolFactory
            .build(options.clone(), no_grant)
            .err()
            .unwrap()
            .reason,
        Reason::InvalidConfig
    );
    let (mut scope, ctx) = context(2);
    let actual = ctx.instance();
    let mut owner = PoolFactory.build(options, ctx).unwrap();
    assert!(pool.managed(actual).is_err());
    owner.lifecycle.start(deadline()).await.unwrap();
    assert!(pool.managed(actual).is_ok());
    assert!(pool
        .managed(ComponentInstanceId {
            generation: zero_core::component::ResourceGeneration(2),
            ..actual
        })
        .is_err());
    let calls = AtomicUsize::new(0);
    let failure = pool
        .open(key("legacy"), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<Arc<crate::client::ClientConnection>, _>(io::Error::other("must not dial"))
        })
        .await
        .err()
        .unwrap();
    assert_eq!(failure.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let end = deadline();
    let (protocol, tasks) = tokio::join!(owner.lifecycle.shutdown(end), scope.shutdown(end));
    assert!(protocol.unwrap().is_complete());
    assert!(tasks.is_complete() && tasks.exited_tasks.is_empty());
}

#[tokio::test]
async fn legacy_inflight_pool_cannot_be_adopted_even_after_cache_clear() {
    let pool = Arc::new(ClientPool::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let observed = Arc::new(tokio::sync::Notify::new());
    let socket = pending_socket(client, observed.clone());
    let clone = pool.clone();
    let caller = tokio::spawn(async move {
        let mut socket = Some(socket);
        clone
            .open(key("old"), || {
                let socket = socket.take().unwrap();
                async move { crate::client::ClientConnection::tcp(socket, "u", "p").await }
            })
            .await
    });
    observed.notified().await;
    let mut opening = [0; 1];
    peer.read_exact(&mut opening).await.unwrap();
    pool.clear();
    let (mut scope, ctx) = context(1);
    let mut native = PoolFactory
        .build(
            Arc::new(pool.component_options(NonZeroUsize::new(1).unwrap())),
            ctx,
        )
        .unwrap();
    assert_eq!(
        native.lifecycle.start(deadline()).await.unwrap_err().reason,
        Reason::StalePlan
    );
    caller.abort();
    let _ = caller.await;
    let end = deadline();
    let (protocol, tasks) = tokio::join!(native.lifecycle.shutdown(end), scope.shutdown(end));
    assert!(protocol.unwrap().is_complete() && tasks.is_complete());
}

#[tokio::test]
async fn pending_real_handshake_uses_owner_service_grant_and_retires_without_recycling_capacity() {
    let pool = Arc::new(ClientPool::default());
    let observed = Arc::new(tokio::sync::Notify::new());
    let opener = Arc::new(Loopback {
        pending: Some(observed.clone()),
        ..Default::default()
    });
    let (mut scope, ctx) = context_with(1, opener.clone());
    let actual = ctx.instance();
    let mut owner = PoolFactory
        .build(
            Arc::new(pool.component_options(NonZeroUsize::new(1).unwrap())),
            ctx,
        )
        .unwrap();
    owner.lifecycle.start(deadline()).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let bound = deadline();
    let clone = pool.clone();
    let caller = tokio::spawn(async move {
        clone
            .open_managed_tcp(
                actual,
                key_at("one", address),
                &Default::default(),
                Some(bound),
            )
            .await
    });
    let (mut peer, _) = listener.accept().await.unwrap();
    observed.notified().await;
    assert_eq!(
        opener.destinations.lock().unwrap().as_slice(),
        &[(
            zero_core::request::Destination::new(
                zero_core::Address::Ipv4([127, 0, 0, 1]),
                address.port()
            ),
            Some(bound)
        )]
    );
    let mut byte = [0; 1];
    peer.read_exact(&mut byte).await.unwrap();
    let error = pool
        .open_managed_tcp(
            actual,
            key_at("two", address),
            &Default::default(),
            Some(bound),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(opener.opens.load(Ordering::SeqCst), 1);
    owner.lifecycle.revoke(CloseReason::Shutdown);
    assert_eq!(
        caller.await.unwrap().err().unwrap().kind(),
        io::ErrorKind::Interrupted
    );
    let mut rest = Vec::new();
    peer.read_to_end(&mut rest).await.unwrap();
    let end = deadline();
    let (protocol, tasks) = tokio::join!(owner.lifecycle.shutdown(end), scope.shutdown(end));
    let report = protocol.unwrap();
    assert!(matches!(report.unfinished.as_slice(), [Unfinished::Io(_)]));
    assert!(report.closed_io.is_empty() && report.exited_tasks.is_empty());
    assert!(tasks.is_complete() && tasks.exited_tasks.is_empty());
}
