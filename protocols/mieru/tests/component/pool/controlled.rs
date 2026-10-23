use super::*;
use std::{future::Future, task::Poll};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

#[tokio::test]
async fn controlled_same_pool_recycles_only_after_actual_scope_join_and_original_native_close() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let pool = Arc::new(ClientPool::default());
        let opener = Arc::new(Loopback::default());
        let (address, server) = echo_connections(2).await;
        let (mut scope, ctx) = controlled_context(1, address, opener.clone());
        let actual = ctx.instance();
        let mut owner = PoolFactory
            .build(
                Arc::new(pool.component_options(NonZeroUsize::new(1).unwrap())),
                ctx,
            )
            .unwrap();
        owner.lifecycle.start(deadline()).await.unwrap();
        let state = pool.managed(actual).unwrap();
        let mut stream = pool
            .open_managed_tcp(
                actual,
                key_at("one", address),
                &Default::default(),
                Some(deadline()),
            )
            .await
            .unwrap();
        stream.write_all(b"first").await.unwrap();
        let mut bytes = [0; 5];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"first");
        pool.clear();
        drop(stream);
        // Observe actual retirement waiting for the join, rather than a delay
        // or the destruction of the protocol future standing in for a receipt.
        loop {
            assert_eq!(
                state.reserve().err().unwrap().kind(),
                io::ErrorKind::WouldBlock
            );
            if !state
                .retire_finished(Instant::now() + Duration::from_millis(10))
                .await
                .unwrap()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(state.physical_unconfirmed());
        let exit = scope.next_exit().await.unwrap();
        assert!(exit.result.is_ok());
        assert!(
            state.physical_unconfirmed(),
            "real join is still not native close"
        );
        assert_eq!(
            state.reserve().err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(state.retire_finished(deadline()).await.unwrap());
        assert!(!state.physical_unconfirmed());
        let mut stream = pool
            .open_managed_tcp(
                actual,
                key_at("two", address),
                &Default::default(),
                Some(deadline()),
            )
            .await
            .unwrap();
        stream.write_all(b"again").await.unwrap();
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"again");
        assert_eq!(opener.opens.load(Ordering::SeqCst), 2);
        drop(stream);
        pool.clear();
        let end = deadline();
        let (protocol, tasks) = tokio::join!(owner.lifecycle.shutdown(end), scope.shutdown(end));
        let report = protocol.unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert_eq!(
            report.closed_io.len(),
            1,
            "the first retirement was already observed, not retained in unbounded history"
        );
        assert!(report.exited_tasks.is_empty());
        assert!(tasks.is_complete());
        assert!(tasks.failed_tasks.is_empty());
        assert_eq!(
            tasks.exited_tasks.len(),
            1,
            "the first exit was already consumed by the actual scope"
        );
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn strict_controlled_owner_default_denies_old_opener_without_legacy_io_or_fake_join() {
    let pool = Arc::new(ClientPool::default());
    let opener = Arc::new(Loopback::default());
    let (mut scope, ctx) = context_with(1, opener.clone());
    let actual = ctx.instance();
    let mut owner = PoolFactory
        .build(
            Arc::new(pool.component_options(NonZeroUsize::new(1).unwrap())),
            ctx,
        )
        .unwrap();
    owner.lifecycle.start(deadline()).await.unwrap();
    for _ in 0..2 {
        let error = pool
            .open_managed_tcp(
                actual,
                key("no-controlled-provider"),
                &Default::default(),
                Some(deadline()),
            )
            .await
            .err()
            .unwrap();
        let error = error
            .get_ref()
            .unwrap()
            .downcast_ref::<ChannelError>()
            .unwrap();
        assert_eq!(error.reason, Reason::Unsupported);
        assert_eq!(error.stage, Stage::Prepare);
        assert_eq!(
            error.delivery,
            zero_platform_tokio::contract::DeliveryState::NotSubmitted
        );
    }
    assert_eq!(opener.opens.load(Ordering::SeqCst), 0);
    let end = deadline();
    let (protocol, tasks) = tokio::join!(owner.lifecycle.shutdown(end), scope.shutdown(end));
    let report = protocol.unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty() && report.exited_tasks.is_empty());
    assert!(tasks.is_complete() && tasks.exited_tasks.is_empty());
}

#[tokio::test]
async fn cancelled_controlled_authentication_closes_original_fd_without_inventing_a_driver_join() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
        let blocked=Arc::new(tokio::sync::Notify::new());let observed=blocked.clone();
        let (finish,finished)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move {
            let (socket,_)=listener.accept().await.unwrap();
            let profile=crate::inbound::MieruInboundProfile::from_config(vec![("u".into(),"p".into())]);
            let mut response=Box::pin(profile.accept_multiplexer(zero_platform_tokio::TcpRelayStream::new(WithheldResponse {socket,blocked})));
            tokio::select! {
                _=response.as_mut()=>panic!("the real authenticated response is withheld"),
                _=finished=>{},
            }
        });
        let pool=Arc::new(ClientPool::default());let opener=Arc::new(Loopback::default());
        let (mut scope,ctx)=controlled_context(1,address,opener.clone());let actual=ctx.instance();
        let mut owner=PoolFactory.build(Arc::new(pool.component_options(NonZeroUsize::new(1).unwrap())),ctx).unwrap();
        owner.lifecycle.start(deadline()).await.unwrap();
        let options=Default::default();
        let mut opening=Box::pin(pool.open_managed_tcp(actual,key_at("pending-auth",address),&options,Some(deadline())));
        tokio::select! {_=opening.as_mut()=>panic!("authentication requires the real peer response"),_=observed.notified()=>{}}
        std::future::poll_fn(|cx|{assert!(opening.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
        assert_eq!(opener.opens.load(Ordering::SeqCst),1);
        owner.lifecycle.revoke(CloseReason::Shutdown);
        assert_eq!(opening.await.err().unwrap().kind(),io::ErrorKind::Interrupted);
        let end=deadline();let (protocol,tasks)=tokio::join!(owner.lifecycle.shutdown(end),scope.shutdown(end));
        let report=protocol.unwrap();assert!(report.is_complete(),"{report:?}");assert_eq!(report.closed_io.len(),1);
        assert!(report.exited_tasks.is_empty());assert!(tasks.is_complete() && tasks.exited_tasks.is_empty());
        finish.send(()).unwrap();server.await.unwrap();
    }).await.unwrap();
}

struct WithheldResponse {
    socket: tokio::net::TcpStream,
    blocked: Arc<tokio::sync::Notify>,
}
impl AsyncRead for WithheldResponse {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_read(cx, buf)
    }
}
impl AsyncWrite for WithheldResponse {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.blocked.notify_one();
        Poll::Pending
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
